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
package org.apache.phoenix.util;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.Format;
import java.util.Properties;

import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.Period;
import org.joda.time.chrono.GJChronology;

public class GMTExpressionContext implements ExpressionContext {

    private final String dateFormatTimeZoneId;

    private final String datePattern;
    private final String timePattern;
    private final String timestampPattern;

    private final Format dateFormatter;
    private final Format timeFormatter;
    private final Format timestampFormatter;
    
    private final Chronology jodaChronology = GJChronology.getInstanceUTC();

    public GMTExpressionContext(String datePattern, String timePattern, String timestampPattern,
            String dateFormatTimeZoneId) {
        this.datePattern = datePattern;
        this.timePattern = timePattern;
        this.timestampPattern = timestampPattern;
        this.dateFormatTimeZoneId = dateFormatTimeZoneId;
        this.dateFormatter = DateUtil.getDateFormatter(datePattern, dateFormatTimeZoneId);
        this.timeFormatter = DateUtil.getTimeFormatter(timePattern, dateFormatTimeZoneId);
        this.timestampFormatter =
                DateUtil.getTimestampFormatter(timestampPattern, dateFormatTimeZoneId);
    }

    @Override
    public Date parseDate(String dateValue) {
        return DateUtil.parseDate(dateValue);
    }

    @Override
    public Time parseTime(String timeValue) {
        return DateUtil.parseTime(timeValue);
    }

    @Override
    public Timestamp parseTimestamp(String timestampValue) {
        return DateUtil.parseTimestamp(timestampValue);
    }
    
    @Override
    public Format getTemporalFormatter(String pattern) {
        return DateUtil.getTemporalFormatter(pattern);
    }

    @Override
    public Format getDateFormatter() {
        return dateFormatter;
    }

    @Override
    public Format getTimeFormatter() {
        return timeFormatter;
    }

    @Override
    public Format getTimestampFormatter() {
        return timestampFormatter;
    }

    @Override
    public String resolveTimezoneId(String timezoneId) {
        return DateUtil.getTimeZone(timezoneId).getID();
    }

    @Override
    public String getTimezoneId() {
        return dateFormatTimeZoneId;
    }

    @Override
    public String getTimestampFormatPattern() {
        return timestampPattern;
    }

    @Override
    public String getDateFormatPattern() {
        return datePattern;
    }

    @Override
    public String getTimeFormatPattern() {
        return timePattern;
    }

    @Override
    public boolean isCompliant() {
        return false;
    }

    @Override
    public int hourImplementation(long epochMs) {
        DateTime dt = new DateTime(epochMs, jodaChronology);
        return dt.getHourOfDay();
    }
    
    @Override
    public int dayOfMonthImplementation(long epochMs) {
        DateTime dt = new DateTime(epochMs, jodaChronology);
        return dt.getDayOfMonth();
    }

    @Override
    public int dayOfWeekImplementation(long epochMs) {
        DateTime dt = new DateTime(epochMs, jodaChronology);
        return dt.getDayOfWeek();
    }

    @Override
    public int dayOfYearImplementation(long epochMs) {
        DateTime dt = new DateTime(epochMs, jodaChronology);
        return dt.getDayOfYear();
    }

    @Override
    public int minuteImplementation(long epochMs) {
        DateTime dt = new DateTime(epochMs, jodaChronology);
        return dt.getMinuteOfHour();
    }

    @Override
    public int secondImplementation(long epochMs) {
        DateTime dt = new DateTime(epochMs, jodaChronology);
        return dt.getSecondOfMinute();
    }

    @Override
    public int yearImplementation(long epochMs) {
        DateTime dt = new DateTime(epochMs, jodaChronology);
        return dt.getYear();
    }

    @Override
    public int weekImplementation(long epochMs) {
        DateTime dt = new DateTime(epochMs, jodaChronology);
        return dt.getWeekOfWeekyear();
    }

    @Override
    public int monthImplementation(long epochMs) {
        DateTime dt = new DateTime(epochMs, jodaChronology);
        return dt.getMonthOfYear();
    }

    
    @Override
    public Properties toProps() {
        return new Properties();
    }

    // Note that the upper/lower functions must be exact, as there is no additional filtering
    // after the PK filtering.
    // In this case we're always using the GMT TZ, and do not have DST, so every interval up to
    // and including days has an standard length.


    // For odd multipliers this will simply halve the intervals on both ends.
    // i.e. for 7 minutes, it's going to round values betwen (n-3:30) and (n+3:30) to n.
    // (where n is a multiple of seven)
    public static long round(long epochMs, long divBy) {
        // Note that this DOES NOT follow the halfEven rounding used elsewhere, and is HalfUp instead
        long roundUpAmount = divBy/2;
        long value;
        if (epochMs <= Long.MAX_VALUE - roundUpAmount) { // If no overflow, add
            value = (epochMs + roundUpAmount) / divBy;
        } else { // Else subtract and add one
            value = (epochMs - roundUpAmount) / divBy + 1;
        }
        return value * divBy;
    }

    public static long roundUpperRange(long epochMs, long divBy) {
        // Note that this DOES NOT follow the halfEven rounding used elsewhere, but uses HalfUp
        return round(epochMs, divBy) + divBy/2 -1; 
    }
    
    public static long roundLowerRange(long epochMs, long divBy) {
        // Note that this DOES NOT follow the halfEven rounding used elsewhere, but uses HalfUp
        return round(epochMs, divBy) - divBy/2; 
    }
    
    public static long floor(long epochMs, long divBy) {
        long roundUpAmount = 0;
        long value;
        if (epochMs <= Long.MAX_VALUE - roundUpAmount) { // If no overflow, add
            value = (epochMs + roundUpAmount) / divBy;
        } else { // Else subtract and add one
            value = (epochMs - roundUpAmount) / divBy + 1;
        }
        return value * divBy;
    }

    public static long ceil(long epochMs, long divBy) {
        long roundUpAmount = divBy -1;
        long value;
        if (epochMs <= Long.MAX_VALUE - roundUpAmount) { // If no overflow, add
            value = (epochMs + roundUpAmount) / divBy;
        } else { // Else subtract and add one
            value = (epochMs - roundUpAmount) / divBy + 1;
        }
        return value * divBy;
    }

    @Override
    public long roundSecond(long epochMs, long divBy, long multiplier) {
        return round(epochMs, divBy);
    }

    @Override
    public long roundSecondLower(long epochMs, long divBy, long multiplier) {
        return roundLowerRange(epochMs, divBy);
    }

    @Override
    public long roundSecondUpper(long epochMs, long divBy, long multiplier) {
        return roundUpperRange(epochMs, divBy);
    }

    @Override
    public long roundMinute(long epochMs, long divBy, long multiplier) {
        return round(epochMs, divBy);
    }

    @Override
    public long roundMinuteLower(long epochMs, long divBy, long multiplier) {
        return roundLowerRange(epochMs, divBy);
    }

    @Override
    public long roundMinuteUpper(long epochMs, long divBy, long multiplier) {
        return roundUpperRange(epochMs, divBy);
    }
    
    @Override
    public long roundHour(long epochMs, long divBy, long multiplier) {
        return round(epochMs, divBy);
    }

    @Override
    public long roundHourLower(long epochMs, long divBy, long multiplier) {
        return roundLowerRange(epochMs, divBy);
    }

    @Override
    public long roundHourUpper(long epochMs, long divBy, long multiplier) {
        return roundUpperRange(epochMs, divBy);
    }

    @Override
    public long roundDay(long epochMs, long divBy, long multiplier) {
        return round(epochMs, divBy);
    }

    @Override
    public long roundDayLower(long epochMs, long divBy, long multiplier) {
        return roundLowerRange(epochMs, divBy);
    }

    @Override
    public long roundDayUpper(long epochMs, long divBy, long multiplier) {
        return roundUpperRange(epochMs, divBy);
    }

    private DateTime roundWeekDT(long epochMs) {
        return (new DateTime(epochMs, jodaChronology))
                .weekOfWeekyear().roundHalfEvenCopy();
    }

    @Override
    public long roundWeek(long epochMs, long divBy, long multiplier) {
        return roundWeekDT(epochMs).getMillis();
    }

    @Override
    public long roundWeekLower(long epochMs, long divBy, long multiplier) {
        DateTime rounded = roundWeekDT(epochMs);
        DateTime prev = rounded.minusWeeks(1);
        return DateUtil.rangeJodaHalfEven(rounded, prev, DateTimeFieldType.weekOfWeekyear());
    }

    @Override
    public long roundWeekUpper(long epochMs, long divBy, long multiplier) {
        DateTime rounded = roundWeekDT(epochMs);
        DateTime next = rounded.plusWeeks(1);
        return DateUtil.rangeJodaHalfEven(rounded, next, DateTimeFieldType.weekOfWeekyear());
    }

    private DateTime roundMonthDT(long epochMs) {
        //Ignores multiplier
        return (new DateTime(epochMs, jodaChronology))
                .monthOfYear().roundHalfEvenCopy();
    }
    
    @Override
    public long roundMonth(long epochMs, long divBy, long multiplier) {
        //Ignores multiplier
        return roundMonthDT(epochMs).getMillis();
    }

    @Override
    public long roundMonthLower(long epochMs, long divBy, long multiplier) {
        DateTime rounded = roundMonthDT(epochMs);
        DateTime prev = rounded.minusMonths(1);
        return DateUtil.rangeJodaHalfEven(rounded, prev, DateTimeFieldType.monthOfYear());
    }

    @Override
    public long roundMonthUpper(long epochMs, long divBy, long multiplier) {
        DateTime rounded = roundMonthDT(epochMs);
        DateTime next = rounded.plusMonths(1);
        return DateUtil.rangeJodaHalfEven(rounded, next, DateTimeFieldType.monthOfYear());
    }

    private DateTime roundYearDT(long epochMs) {
        //Ignores multiplier
        return (new DateTime(epochMs, jodaChronology))
                .year().roundHalfEvenCopy();
    }

    @Override
    public long roundYear(long epochMs, long divBy, long multiplier) {
        //Ignores multiplier
        return roundYearDT(epochMs).getMillis();
    }

    @Override
    public long roundYearLower(long epochMs, long divBy, long multiplier) {
        DateTime rounded = roundYearDT(epochMs);
        DateTime prev = rounded.minusYears(1);
        return DateUtil.rangeJodaHalfEven(rounded, prev, DateTimeFieldType.year());
    }

    @Override
    public long roundYearUpper(long epochMs, long divBy, long multiplier) {
        DateTime rounded = roundYearDT(epochMs);
        DateTime next = rounded.plusYears(1);
        return DateUtil.rangeJodaHalfEven(rounded, next, DateTimeFieldType.year());
    }

    @Override
    public long floorSecond(long epochMs, long divBy, long multiplier) {
        return floor(epochMs, divBy);
    }

    @Override
    public long floorMinute(long epochMs, long divBy, long multiplier) {
        return floor(epochMs, divBy);
    }

    @Override
    public long floorHour(long epochMs, long divBy, long multiplier) {
        return floor(epochMs, divBy);
    }

    @Override
    public long floorDay(long epochMs, long divBy, long multiplier) {
        return floor(epochMs, divBy);
    }

    @Override
    public long floorWeek(long epochMs, long divBy, long multiplier) {
        //Ignores multiplier
        return (new DateTime(epochMs, jodaChronology))
                .weekOfWeekyear().roundFloorCopy().getMillis();
    }

    @Override
    public long floorMonth(long epochMs, long divBy, long multiplier) {
        //Ignores multiplier
        return (new DateTime(epochMs, jodaChronology))
                .monthOfYear().roundFloorCopy().getMillis();
    }

    @Override
    public long floorYear(long epochMs, long divBy, long multiplier) {
        //Ignores multiplier
        return (new DateTime(epochMs, jodaChronology))
                .year().roundFloorCopy().getMillis();
    }

    @Override
    public long ceilSecond(long epochMs, long divBy, long multiplier) {
        return ceil(epochMs, divBy);
    }

    @Override
    public long ceilMinute(long epochMs, long divBy, long multiplier) {
        return ceil(epochMs, divBy);
    }

    @Override
    public long ceilHour(long epochMs, long divBy, long multiplier) {
        return ceil(epochMs, divBy);
    }

    @Override
    public long ceilDay(long epochMs, long divBy, long multiplier) {
        return ceil(epochMs, divBy);
    }

    @Override
    public long ceilWeek(long epochMs, long divBy, long multiplier) {
        //Ignores multiplier
        return (new DateTime(epochMs, jodaChronology))
                .weekOfWeekyear().roundCeilingCopy().getMillis();
    }

    @Override
    public long ceilMonth(long epochMs, long divBy, long multiplier) {
        //Ignores multiplier
        return (new DateTime(epochMs, jodaChronology))
                .monthOfYear().roundCeilingCopy().getMillis();
    }

    @Override
    public long ceilYear(long epochMs, long divBy, long multiplier) {
        //Ignores multiplier
        return (new DateTime(epochMs, jodaChronology))
                .year().roundCeilingCopy().getMillis();
    }
}
