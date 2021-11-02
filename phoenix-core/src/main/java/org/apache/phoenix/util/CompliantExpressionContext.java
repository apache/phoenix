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
import java.text.ParseException;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PTime;
import org.apache.phoenix.schema.types.PTimestamp;
import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.Instant;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.Minutes;
import org.joda.time.Period;
import org.joda.time.Seconds;
import org.joda.time.chrono.GJChronology;

public class CompliantExpressionContext implements ExpressionContext {

    private final String timeZoneId;
    private final TimeZone timeZone;
    private final DateTimeZone jodaTZ;
    private final Chronology jodaChronology;

    private String datePattern = QueryServicesOptions.DEFAULT_MS_DATE_FORMAT;
    private String timePattern = QueryServicesOptions.DEFAULT_MS_DATE_FORMAT;
    private String timestampPattern = QueryServicesOptions.DEFAULT_MS_DATE_FORMAT;

    private final FastDateFormat dateFormatter;
    private final FastDateFormat timeFormatter;
    private final FastDateFormat timestampFormatter;
    
    private final org.apache.phoenix.util.DateUtil.DateTimeParser dateParser; 
    private final org.apache.phoenix.util.DateUtil.DateTimeParser timeParser; 
    private final org.apache.phoenix.util.DateUtil.DateTimeParser timestampParser;

    private final static LocalDateTime EPOCH_LDT = Instant.EPOCH.toDateTime(GJChronology.getInstanceUTC()).toLocalDateTime();
    private final static LocalDate EPOCH_LD = Instant.EPOCH.toDateTime(GJChronology.getInstanceUTC()).toLocalDate();

    public CompliantExpressionContext(String datePattern, String timePattern, String timestampPattern,
            String timeZoneIdIn) {

        // Timezones are are as usual a pain.
        // java.util.Date and java.util.calendar interpret the three letter time zones literally,
        // i.e. "EST" means EST, without DST. 
        // Joda maps the three litter IDs to an actual place, i.e. America/New_York, which DOES use
        // DST (as of this writing) (This is a highly debatable behaviour by Joda)
        // to get around this, we need normalize the user provided timezone to whatever Joda 
        // maps it to, and use it for the legacy Date handling classes.
        //
        // When we move to java.time, we need to revisit this.

        //TODO should we just disallow LOCAL as the timezone value? 
        if(timeZoneIdIn == null || timeZoneIdIn.equals(DateUtil.LOCAL_TIME_ZONE_ID)) {
            this.timeZone = TimeZone.getDefault();
            this.timeZoneId = timeZone.getID();
        } else {
            this.timeZoneId = normalizeTimeZoneIdWithJoda(timeZoneIdIn);
            this.timeZone = TimeZone.getTimeZone(timeZoneId);
        }

        //This enables us to use short IDs.
        this.jodaTZ = DateTimeZone.forTimeZone(timeZone);
        this.jodaChronology = GJChronology.getInstance(jodaTZ);
        
        if (datePattern != null) {
            this.datePattern = datePattern;
        }
        if (timePattern != null) {
            this.timePattern = timePattern;
        }
        if (timestampPattern != null) {
            this.timestampPattern = timestampPattern;
        }

        //FIXME just use a single java.time parser/formatter object.
        this.dateFormatter = DateUtil.getDateFormatter(this.datePattern, timeZoneId);
        this.dateParser = DateUtil.getTemporalParser(datePattern, PDate.INSTANCE, timeZoneId);
        this.timeFormatter = DateUtil.getTimeFormatter(this.timePattern, timeZoneId);
        this.timeParser = DateUtil.getTemporalParser(datePattern, PTime.INSTANCE, timeZoneId);
        this.timestampFormatter = DateUtil.getTimestampFormatter(this.timestampPattern, this.timeZoneId);
        this.timestampParser = DateUtil.getTemporalParser(datePattern, PTimestamp.INSTANCE, timeZoneId);
    }

    private String normalizeTimeZoneIdWithJoda(String timeZoneId) {
        return DateTimeZone.forTimeZone(TimeZone.getTimeZone(timeZoneId)).getID();
    }
    
    //FIXME Use java.time
    @Override
    public Date parseDate(String dateValue) {
        Pair<String, Integer> fixed = DateUtil.fixNanos(dateValue);
        Date date = new Date(dateParser.parseDateTime(fixed.getFirst()));
        return date;
    }

   //FIXME Use java.time
    @Override
    public Time parseTime(String timeValue) {
        Pair<String, Integer> fixed = DateUtil.fixNanos(timeValue);
        Time timestamp = new Time(dateParser.parseDateTime(fixed.getFirst()));
        return timestamp;
    }

   //FIXME Use java.time
    @Override
    public Timestamp parseTimestamp(String timestampValue) {
        Pair<String, Integer> fixed = DateUtil.fixNanos(timestampValue);
        Timestamp timestamp = new Timestamp(dateParser.parseDateTime(fixed.getFirst()));
        Integer nanos = fixed.getSecond();
        if (nanos != null) {
            timestamp.setNanos(nanos);
        }
        return timestamp;
    }

    @Override
    public Format getTemporalFormatter(String pattern) {
        return DateUtil.getTemporalFormatter(pattern, timeZoneId);
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
    // This controls the behaviour of the "LOCAL" timezone
    // While there's not much point, it is possible to set an Explicit TZ, and override
    // it in a function argument with LOCAL
    public String resolveTimezoneId(String rTimeZoneId) {
        if (DateUtil.LOCAL_TIME_ZONE_ID.equalsIgnoreCase(rTimeZoneId)) {
            return TimeZone.getDefault().getID();
        } else {
            return rTimeZoneId;
        }
    }

    @Override
    public String getTimezoneId() {
        return timeZoneId;
    }


    // FIXME do formatting in this class instead
    @Deprecated
    @Override
    public String getTimestampFormatPattern() {
        return timestampPattern;
    }

    // FIXME do formatting in this class instead
    @Deprecated
    @Override
    public String getDateFormatPattern() {
        return datePattern;
    }


    // FIXME do formatting in this class instead
    @Deprecated
    @Override
    public String getTimeFormatPattern() {
        return timePattern;
    }


    @Override
    public boolean isCompliant() {
        return true;
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
        Properties props = new Properties();
        props.setProperty(QueryServices.JDBC_COMPLIANT_TZ_HANDLING, "true");
        props.setProperty(QueryServices.TIMEZONE_OVERRIDE, timeZoneId);
        if (!datePattern.equals(QueryServicesOptions.DEFAULT_MS_DATE_FORMAT)) {
            props.setProperty(QueryServices.TIMESTAMP_FORMAT_ATTRIB, timestampPattern);
        }
        if (!timePattern.equals(QueryServicesOptions.DEFAULT_MS_DATE_FORMAT)) {
            props.setProperty(QueryServices.TIME_FORMAT_ATTRIB, timePattern);
        }
        if (!datePattern.equals(QueryServicesOptions.DEFAULT_MS_DATE_FORMAT)) {
            props.setProperty(QueryServices.DATE_FORMAT_ATTRIB, datePattern);
        }
        return props;
    }

    // Handling rounding to multiples of time units is not well defined when dealing with time
    // zones that have DST changes (and possibly other complications).
    // We luckily don't implement multiples for weeks, month and years
    // (the latter two obviously don't have standard lengths), but even days can be 23 or 25 
    // hour longs, and even adding one hour to a timestamp may does not necessarily mean that the
    // new date has the same minutes as the old one (see Lord Howe Island DST)
    
    private int diffToMultipleRound(long value, long multiple) {
        //Note that this always rounds down
        long roundDown = multiple / 2;
        long rounded = multiple * ((value + roundDown) / multiple);
        return (int)(rounded - value);
    }

    private int diffToMultipleFloor(long value, long multiple) {
        long rounded = multiple * (value / multiple);
        return (int)(rounded - value);
    }

    private int diffToMultipleCeil(long value, long multiple) {
        long roundUp = multiple -1;
        long rounded = multiple * ((value + roundUp) / multiple);
        return (int)(rounded - value);
    }

    // TODO Should we use offsets to re-interpret the date as GMT, use the old logic, then
    // apply the offset again instead of the the multiplier logic below for minute/hour/day ?

    private DateTime roundSecondDt(long epochMs, long multiplier) {
        // FIXME seconds are not affected by DST
        // Use simple ms add/subtract where possible
        DateTime dt = new DateTime(epochMs, jodaChronology);
        if (multiplier == 1) {
            dt = dt.secondOfDay().roundHalfCeilingCopy();
            return dt;
        } else {
            if (multiplier % 2 == 1) {
                //odd multiplier range is [-m/2*1000+500, +m/2*1000+499] ms
                dt = dt.secondOfDay().roundHalfCeilingCopy();
            } else {
                //even multiplier range is [-m/2*1000, +m/2*1000-1] ms
                dt = dt.secondOfDay().roundFloorCopy();
            }
            int secondsSinceEpoch = Seconds.secondsBetween(EPOCH_LDT, dt.toLocalDateTime()).getSeconds();
            int diffSecondsOfDay = diffToMultipleRound(secondsSinceEpoch, multiplier);
            return dt.plusSeconds(diffSecondsOfDay);
        }
    }

    @Override
    public long roundSecond(long epochMs, long divBy, long multiplier) {
        return roundSecondDt(epochMs, multiplier).getMillis();
    }

    @Override
    public long roundSecondLower(long epochMs, long divBy, long multiplier) {
        DateTime rounded = roundSecondDt(epochMs, multiplier);
        if (multiplier == 1) {
            DateTime prev = rounded.minusSeconds(1);
            return DateUtil.rangeJodaHalfCeiling(rounded, prev, DateTimeFieldType.secondOfDay());
        } else if (multiplier % 2 == 1) {
            //odd
            DateTime prev = rounded.minusSeconds((int)multiplier/2);
            return DateUtil.rangeJodaHalfCeiling(prev, prev.minusSeconds(1), DateTimeFieldType.secondOfDay());
        } else {
            //even
            return rounded.minusSeconds((int)multiplier/2).getMillis();
        }
     }

    @Override
    public long roundSecondUpper(long epochMs, long divBy, long multiplier) {
        DateTime rounded = roundSecondDt(epochMs, multiplier);
        if (multiplier == 1) {
            DateTime next = rounded.plusSeconds(1);
            return DateUtil.rangeJodaHalfCeiling(rounded, next, DateTimeFieldType.secondOfDay());
        } else if (multiplier % 2 == 1) {
            //odd
            DateTime next = rounded.plusSeconds((int)multiplier/2);
            return DateUtil.rangeJodaHalfCeiling(next, next.plusSeconds(1), DateTimeFieldType.secondOfDay());
        } else {
            //even
            return rounded.plusSeconds((int)multiplier/2).getMillis() -1;
        }
    }

    private DateTime roundMinuteDt(long epochMs, long multiplier) {
        // FIXME minutes are not affected by DST
        // Use simple ms add/subtract where possible
        DateTime dt = new DateTime(epochMs, jodaChronology);
        if (multiplier == 1) {
            dt = dt.minuteOfDay().roundHalfCeilingCopy();
            return dt;
        } else {
            if (multiplier % 2 == 1) {
                //odd multiplier
                dt = dt.minuteOfDay().roundHalfCeilingCopy();
            } else {
                //even multiplier
                dt = dt.minuteOfDay().roundFloorCopy();
            }
            int minutesSinceEpoch = Minutes.minutesBetween(EPOCH_LDT, dt.toLocalDateTime()).getMinutes();
            int diffMinutes = diffToMultipleRound(minutesSinceEpoch, multiplier);
            return dt.plusMinutes(diffMinutes);
        }
    }

    @Override
    public long roundMinute(long epochMs, long divBy, long multiplier) {
        return roundMinuteDt(epochMs, multiplier).getMillis();
    }

    @Override
    public long roundMinuteLower(long epochMs, long divBy, long multiplier) {
        DateTime rounded = roundMinuteDt(epochMs, multiplier);
        if (multiplier == 1) {
            DateTime prev = rounded.minusMinutes(1);
            return DateUtil.rangeJodaHalfCeiling(rounded, prev, DateTimeFieldType.minuteOfDay());
        } else if (multiplier % 2 == 1) {
            //odd
            DateTime prev = rounded.minusMinutes((int)multiplier/2);
            return DateUtil.rangeJodaHalfCeiling(prev, prev.minusMinutes(1), DateTimeFieldType.minuteOfDay());
        } else {
            //even
            return rounded.minusMinutes((int)multiplier/2).getMillis();
        }
    }

    @Override
    public long roundMinuteUpper(long epochMs, long divBy, long multiplier) {
        DateTime rounded = roundMinuteDt(epochMs, multiplier);
        if (multiplier == 1) {
            DateTime next = rounded.plusMinutes(1);
            return DateUtil.rangeJodaHalfCeiling(rounded, next, DateTimeFieldType.minuteOfDay());
        } else if (multiplier % 2 == 1) {
            //odd
            DateTime next = rounded.plusMinutes((int)multiplier/2);
            return DateUtil.rangeJodaHalfCeiling(next, next.plusMinutes(1), DateTimeFieldType.minuteOfDay());
        } else {
            //even
            return rounded.plusMinutes((int)multiplier/2).getMillis() -1;
        }
    }

    private DateTime roundHourDt(long epochMs,long multiplier) {
        // FIXME hours are not affected by DST
        // Use simple ms add/subtract where possible
        DateTime dt = new DateTime(epochMs, jodaChronology);
        if (multiplier == 1) {
            dt = dt.hourOfDay().roundHalfCeilingCopy();
            return dt;
        } else {
            if (multiplier % 2 == 1) {
                //odd multiplier
                dt = dt.hourOfDay().roundHalfCeilingCopy();
            } else {
                //even multiplier
                dt = dt.hourOfDay().roundFloorCopy();
            }
            int hoursSinceEpoch = Hours.hoursBetween(EPOCH_LDT, dt.toLocalDateTime()).getHours();
            int diffHours = diffToMultipleRound(hoursSinceEpoch, multiplier);
            return dt.plusHours(diffHours);
        }
    }

    @Override
    public long roundHour(long epochMs, long divBy, long multiplier) {
        return roundHourDt(epochMs, multiplier).getMillis();
    }

    @Override
    public long roundHourLower(long epochMs, long divBy, long multiplier) {
        DateTime rounded = roundHourDt(epochMs, multiplier);
        if (multiplier == 1) {
            DateTime prev = rounded.minusHours(1);
            return DateUtil.rangeJodaHalfCeiling(rounded, prev, DateTimeFieldType.hourOfDay());
        } else if (multiplier % 2 == 1) {
            //odd
            DateTime prev = rounded.minusHours((int)multiplier/2);
            return DateUtil.rangeJodaHalfCeiling(prev, prev.minusHours(1), DateTimeFieldType.hourOfDay());
        } else {
            //even
            return rounded.minusHours((int)multiplier/2).getMillis();
        }
    }

    @Override
    public long roundHourUpper(long epochMs, long divBy, long multiplier) {
        DateTime rounded = roundHourDt(epochMs, multiplier);
        if (multiplier == 1) {
            DateTime next = rounded.plusHours(1);
            return DateUtil.rangeJodaHalfCeiling(rounded, next, DateTimeFieldType.hourOfDay());
        } else if (multiplier % 2 == 1) {
            //odd
            DateTime next = rounded.plusHours((int)multiplier/2);
            return DateUtil.rangeJodaHalfCeiling(next, next.plusHours(1), DateTimeFieldType.hourOfDay());
        } else {
            //even
            return rounded.plusHours((int)multiplier/2).getMillis() -1;
        }
    }

    private DateTime roundDayDt(long epochMs,long multiplier) {
        //Date is the only place where we actually need to use the
        //joda date arithmetic to handle DST. Smaller periods don't consider DST in Joda, and
        //we thankfully don't support multiples for larger periods.
        DateTime dt = new DateTime(epochMs, jodaChronology);
        if (multiplier == 1) {
            dt = dt.dayOfMonth().roundHalfCeilingCopy();
            return dt;
        } else {
            if (multiplier % 2 == 1) {
                //odd multiplier
                dt = dt.dayOfMonth().roundHalfCeilingCopy();
            } else {
                //even multiplier
                dt = dt.dayOfMonth().roundFloorCopy();
            }
            int daysSinceEpoch = Days.daysBetween(EPOCH_LDT, dt.toLocalDateTime()).getDays();
            int diffDays = diffToMultipleRound(daysSinceEpoch, multiplier);
            return dt.plusDays(diffDays);
        }
    }

    @Override
    public long roundDay(long epochMs, long divBy, long multiplier) {
        return roundDayDt(epochMs, multiplier).getMillis();
    }

    @Override
    public long roundDayLower(long epochMs, long divBy, long multiplier) {
        DateTime rounded = roundDayDt(epochMs, multiplier);
        if (multiplier == 1) {
            DateTime prev = rounded.minusDays(1);
            return DateUtil.rangeJodaHalfCeiling(rounded, prev, DateTimeFieldType.dayOfMonth());
        } else if (multiplier % 2 == 1) {
            //odd
            DateTime prev = rounded.minusDays((int)multiplier/2);
            return DateUtil.rangeJodaHalfCeiling(prev, prev.minusDays(1), DateTimeFieldType.dayOfMonth());
        } else {
            //even
            return rounded.minusDays((int)multiplier/2).getMillis();
        }
    }

    @Override
    public long roundDayUpper(long epochMs, long divBy, long multiplier) {
        DateTime rounded = roundDayDt(epochMs, multiplier);
        if (multiplier == 1) {
            DateTime next = rounded.plusDays(1);
            return DateUtil.rangeJodaHalfCeiling(rounded, next, DateTimeFieldType.dayOfMonth());
        } else if (multiplier % 2 == 1) {
            //odd
            DateTime next = rounded.plusDays((int)multiplier/2);
            return DateUtil.rangeJodaHalfCeiling(next, next.plusDays(1), DateTimeFieldType.dayOfMonth());
        } else {
            //even
            return rounded.plusDays((int)multiplier/2).getMillis() -1;
        }
    }

    private DateTime roundWeekDt(long epochMs) {
        //Ignores multiplier
        return (new DateTime(epochMs, jodaChronology))
                .weekOfWeekyear().roundHalfCeilingCopy();
    }
    
    @Override
    public long roundWeek(long epochMs, long divBy, long multiplier) {
        //Ignores multiplier
        return roundWeekDt(epochMs).getMillis();
    }

    @Override
    public long roundWeekLower(long epochMs, long divBy, long multiplier) {
        DateTime rounded = roundWeekDt(epochMs);
        DateTime prev = rounded.minusWeeks(1);
        return DateUtil.rangeJodaHalfCeiling(rounded, prev, DateTimeFieldType.weekOfWeekyear());
    }

    @Override
    public long roundWeekUpper(long epochMs, long divBy, long multiplier) {
        DateTime rounded = roundWeekDt(epochMs);
        DateTime next = rounded.plusWeeks(1);
        return DateUtil.rangeJodaHalfCeiling(rounded, next, DateTimeFieldType.weekOfWeekyear());
    }

    private DateTime roundMonthDt(long epochMs) {
        //Ignores multiplier
        return (new DateTime(epochMs, jodaChronology))
                .monthOfYear().roundHalfCeilingCopy();
    }
    
    @Override
    public long roundMonth(long epochMs, long divBy, long multiplier) {
        //Ignores multiplier
        return roundMonthDt(epochMs).getMillis();
    }

    @Override
    public long roundMonthLower(long epochMs, long divBy, long multiplier) {
        DateTime rounded = roundMonthDt(epochMs);
        DateTime prev = rounded.minusMonths(1);
        return DateUtil.rangeJodaHalfCeiling(rounded, prev, DateTimeFieldType.monthOfYear());
    }

    @Override
    public long roundMonthUpper(long epochMs, long divBy, long multiplier) {
        DateTime rounded = roundMonthDt(epochMs);
        DateTime next = rounded.plusMonths(1);
        return DateUtil.rangeJodaHalfCeiling(rounded, next, DateTimeFieldType.monthOfYear());
    }

    private DateTime roundYearDt(long epochMs) {
        //Ignores multiplier
        return (new DateTime(epochMs, jodaChronology))
                .year().roundHalfCeilingCopy();
    }
    
    @Override
    public long roundYear(long epochMs, long divBy, long multiplier) {
        //Ignores multiplier
        return roundYearDt(epochMs).getMillis();
    }

    @Override
    public long roundYearLower(long epochMs, long divBy, long multiplier) {
        DateTime rounded = roundMonthDt(epochMs);
        DateTime prev = rounded.minusYears(1);
        return DateUtil.rangeJodaHalfCeiling(rounded, prev, DateTimeFieldType.year());
    }

    @Override
    public long roundYearUpper(long epochMs, long divBy, long multiplier) {
        DateTime rounded = roundMonthDt(epochMs);
        DateTime next = rounded.plusYears(1);
        return DateUtil.rangeJodaHalfCeiling(rounded, next, DateTimeFieldType.year());
    }

    @Override
    public long floorSecond(long epochMs, long divBy, long multiplier) {
        // FIXME seconds are not affected by timezone/Chronology. 
        // Can we just use the faster ? GMT implementation ?
        DateTime dt = new DateTime(epochMs, jodaChronology);
        dt = dt.secondOfDay().roundFloorCopy();
        if (multiplier == 1) {
            return dt.getMillis();
        } else {
            int secondsSinceEpoch = Seconds.secondsBetween(EPOCH_LDT, dt.toLocalDateTime()).getSeconds();
            int diffSecondsOfDay = diffToMultipleFloor(secondsSinceEpoch, multiplier);
            return dt.plusSeconds(diffSecondsOfDay).getMillis();
        }
    }

    @Override
    public long floorMinute(long epochMs, long divBy, long multiplier) {
        DateTime dt = new DateTime(epochMs, jodaChronology);
        dt = dt.minuteOfDay().roundFloorCopy();
        if (multiplier == 1) {
            return dt.getMillis();
        } else {
            int minutesSinceEpoch = Minutes.minutesBetween(EPOCH_LDT, dt.toLocalDateTime()).getMinutes();
            int diffMinutesSinceEpoch = diffToMultipleFloor(minutesSinceEpoch, multiplier);
            return dt.plusMinutes(diffMinutesSinceEpoch).getMillis();
        }
    }

    @Override
    public long floorHour(long epochMs, long divBy, long multiplier) {
        DateTime dt = new DateTime(epochMs, jodaChronology);
        dt = dt.hourOfDay().roundFloorCopy();
        if (multiplier == 1) {
            return dt.getMillis();
        } else {
            int hoursSinceEpoch = Hours.hoursBetween(EPOCH_LDT, dt.toLocalDateTime()).getHours();
            int diffHoursSinceEpoch = diffToMultipleFloor(hoursSinceEpoch, multiplier);
            return dt.plusHours(diffHoursSinceEpoch).getMillis();
        }
    }

    @Override
    public long floorDay(long epochMs, long divBy, long multiplier) {
        DateTime dt = new DateTime(epochMs, jodaChronology);
        dt = dt.dayOfMonth().roundFloorCopy();
        if (multiplier == 1) {
            return dt.getMillis();
        } else {
            int daysSinceEpoch = Days.daysBetween(EPOCH_LD, dt.toLocalDate()).getDays();
            int diffDaysSinceEpoch = diffToMultipleFloor(daysSinceEpoch, multiplier);
            return dt.plusDays(diffDaysSinceEpoch).getMillis();
        }
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
        // FIXME seconds are not affected by timezone/Chronology. 
        // Can we just use the faster ? GMT implementation ?
        DateTime dt = new DateTime(epochMs, jodaChronology);
        dt = dt.secondOfDay().roundCeilingCopy();
        if (multiplier == 1) {
            return dt.getMillis();
        } else {
            int secondsSinceEpoch = Seconds.secondsBetween(EPOCH_LDT, dt.toLocalDateTime()).getSeconds();
            int diffSecondsOfDay = diffToMultipleCeil(secondsSinceEpoch, multiplier);
            return dt.plusSeconds(diffSecondsOfDay).getMillis();
        }
    }

    @Override
    public long ceilMinute(long epochMs, long divBy, long multiplier) {
        DateTime dt = new DateTime(epochMs, jodaChronology);
        dt = dt.minuteOfDay().roundCeilingCopy();
        if (multiplier == 1) {
            return dt.getMillis();
        } else {
            int minutesSinceEpoch = Minutes.minutesBetween(EPOCH_LDT, dt.toLocalDateTime()).getMinutes();
            int diffMinutesSinceEpoch = diffToMultipleCeil(minutesSinceEpoch, multiplier);
            return dt.plusMinutes(diffMinutesSinceEpoch).getMillis();
        }
    }

    @Override
    public long ceilHour(long epochMs, long divBy, long multiplier) {
        DateTime dt = new DateTime(epochMs, jodaChronology);
        dt = dt.hourOfDay().roundCeilingCopy();
        if (multiplier == 1) {
            return dt.getMillis();
        } else {
            int hoursSinceEpoch = Hours.hoursBetween(EPOCH_LDT, dt.toLocalDateTime()).getHours();
            int diffHoursSinceEpoch = diffToMultipleCeil(hoursSinceEpoch, multiplier);
            return dt.plusHours(diffHoursSinceEpoch).getMillis();
        }
    }

    @Override
    public long ceilDay(long epochMs, long divBy, long multiplier) {
        DateTime dt = new DateTime(epochMs, jodaChronology);
        dt = dt.dayOfMonth().roundCeilingCopy();
        if (multiplier == 1) {
            return dt.getMillis();
        } else {
            int daysSinceEpoch = Days.daysBetween(EPOCH_LD, dt.toLocalDate()).getDays();
            int diffDaysSinceEpoch = diffToMultipleCeil(daysSinceEpoch, multiplier);
            return dt.plusDays(diffDaysSinceEpoch).getMillis();
        }
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
