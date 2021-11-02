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

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.types.PDataType;
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.GJChronology;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.ISODateTimeFormat;


@SuppressWarnings({ "serial"})
public class DateUtil {
    public static final String DEFAULT_TIME_ZONE_ID = "GMT";
    public static final String LOCAL_TIME_ZONE_ID = "LOCAL";
    private static final TimeZone DEFAULT_TIME_ZONE = TimeZone.getTimeZone(DEFAULT_TIME_ZONE_ID);
    
    public static final FastDateFormat DEFAULT_MS_DATE_FORMATTER = FastDateFormat.getInstance(
            QueryServicesOptions.DEFAULT_MS_DATE_FORMAT, TimeZone.getTimeZone(DEFAULT_TIME_ZONE_ID));

    //TODO most methods here should be moved to either ExpressionContext implementation
    private static final DateTimeFormatter JULIAN_DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
        .append(ISODateTimeFormat.dateParser())
        .appendOptional(new DateTimeFormatterBuilder()
                .appendLiteral(' ').toParser())
        .appendOptional(new DateTimeFormatterBuilder()
                .append(ISODateTimeFormat.timeParser()).toParser())
        .toFormatter().withChronology(GJChronology.getInstanceUTC());

    private DateUtil() {
    }

    public static boolean isResolveTimezone(String timeZoneId) {
        return DateUtil.LOCAL_TIME_ZONE_ID.equalsIgnoreCase(timeZoneId);
    }

    public static TimeZone getTimeZone(String timeZoneId) {
        TimeZone parserTimeZone;
        if (timeZoneId == null || timeZoneId.equals(DateUtil.DEFAULT_TIME_ZONE_ID)) {
            parserTimeZone = DateUtil.DEFAULT_TIME_ZONE;
        } else if (LOCAL_TIME_ZONE_ID.equalsIgnoreCase(timeZoneId)) {
            parserTimeZone = TimeZone.getDefault();
        } else {
            parserTimeZone = TimeZone.getTimeZone(timeZoneId);
        }
        return parserTimeZone;
    }

    private static String getDefaultFormat(PDataType type) {
        return QueryServicesOptions.DEFAULT_MS_DATE_FORMAT;
    }

    @Deprecated
    public static DateTimeParser getTemporalParser(String pattern, PDataType pDataType, String timeZoneId) {
        TimeZone timeZone = getTimeZone(timeZoneId);
        String defaultPattern = getDefaultFormat(pDataType);
        if (pattern == null || pattern.length() == 0) {
            pattern = defaultPattern;
        }
        if(defaultPattern.equals(pattern)) {
            return JulianDateFormatParserFactory.getParser(timeZone);
        } else {
            return new SimpleDateFormatParser(pattern, timeZone);
        }
    }

    public static Format getTemporalFormatter(String pattern) {
        return getTemporalFormatter(pattern, DateUtil.DEFAULT_TIME_ZONE_ID);
    }

    public static FastDateFormat getTemporalFormatter(String pattern, String timeZoneID) {
        return QueryServicesOptions.DEFAULT_MS_DATE_FORMAT.equals(pattern) && DateUtil.DEFAULT_TIME_ZONE_ID.equals(timeZoneID)
                ? DateUtil.DEFAULT_MS_DATE_FORMATTER
                : FastDateFormat.getInstance(pattern, getTimeZone(timeZoneID));
    }

    //FIXME any reason to keep this three ?
    public static FastDateFormat getDateFormatter(String pattern, String timeZoneID) {
        return getTemporalFormatter(pattern, timeZoneID);
    }

    public static FastDateFormat getTimeFormatter(String pattern, String timeZoneID) {
        return getTemporalFormatter(pattern, timeZoneID);
    }

    public static FastDateFormat getTimestampFormatter(String pattern, String timeZoneID) {
        return getTemporalFormatter(pattern, timeZoneID);
    }

    private static long parseDateTime(String dateTimeValue) {
        return JulianDateFormatParser.getInstance().parseDateTime(dateTimeValue);
    }

    public static Date parseDate(String dateValue) {
        return new Date(parseDateTime(dateValue));
    }

    public static Time parseTime(String timeValue) {
        return new Time(parseDateTime(timeValue));
    }

    // Heuristics to parse and remove nanoseconds from Date string.
    // FIXME the java.time refactor should make this unnecessary
    public static Pair<String, Integer> fixNanos(String temporalString) {
        int periodPos = temporalString.lastIndexOf('.');
        if (periodPos == -1) {
            return new Pair<String, Integer>(temporalString, null);
        }
        int checkPos = periodPos + 1;
        StringBuilder fractionalStr = new StringBuilder();
        int totalLength = temporalString.length();
        while (checkPos < totalLength) {
           char c =  temporalString.charAt(checkPos++);
           if(c >= '0' && c <= '9') {
               fractionalStr.append(c);
           } else {
               break;
           }
        }
        if (fractionalStr.length() <= 3) {
            //3 or less fractional digits found
            return new Pair<String, Integer>(temporalString, null);
        } else if (fractionalStr.length() > 9) {
            throw new IllegalDataException("nanos > 999999999 or < 0");
        } else {
            StringBuilder ret = new StringBuilder(temporalString.substring(0, periodPos + 4));
            ret.append(temporalString.substring(checkPos));
            int nanos = Integer.valueOf(fractionalStr.toString());
            for (int c=0; c < 9-fractionalStr.length(); c++) {
                nanos = nanos * 10;
            }
            return new Pair<String, Integer>(ret.toString(), nanos); 
        }
    }

    // FIXME Use a sane parsing library
    public static Timestamp parseTimestamp(String timestampValue) {
        Pair<String, Integer> fixed = DateUtil.fixNanos(timestampValue);
        Timestamp timestamp = new Timestamp(parseDateTime(fixed.getFirst()));
        Integer nanos = fixed.getSecond();
        if (nanos != null) {
            timestamp.setNanos(fixed.getSecond());
        }
        return timestamp;
    }

    //FIXME kill it with fire
    //Move the functionality to ExpressionContext
    //This interface inherently loses nanos, which breaks most TIMESTAMP ops
    public static interface DateTimeParser {
        public long parseDateTime(String dateTimeString) throws IllegalDataException;
        public TimeZone getTimeZone();
    }

    /**
     * This class is used when a user explicitly provides phoenix.query.dateFormat in configuration
     */
    private static class SimpleDateFormatParser implements DateTimeParser {
        private String datePattern;
        private SimpleDateFormat parser;

        public SimpleDateFormatParser(String pattern, TimeZone timeZone) {
            datePattern = pattern;
            parser = new SimpleDateFormat(pattern) {
                @Override
                public java.util.Date parseObject(String source) throws ParseException {
                    java.util.Date date = super.parse(source);
                    return new java.sql.Date(date.getTime());
                }
            };
            parser.setTimeZone(timeZone);
        }

        
        @Override
        public long parseDateTime(String dateTimeString) throws IllegalDataException {
            try {
                Pair<String, Integer> fixed = fixNanos(dateTimeString);
                return parser.parse(fixed.getFirst()).getTime();
            } catch (ParseException e) {
                throw new IllegalDataException("Unable to parse date/time '" + dateTimeString + "' using format string of '" + datePattern + "'.");
            }
        }

        @Override
        public TimeZone getTimeZone() {
            return parser.getTimeZone();
        }
    }

    private static class JulianDateFormatParserFactory {
        private JulianDateFormatParserFactory() {}

        public static DateTimeParser getParser(final TimeZone timeZone) {
            // If timeZone matches default, get singleton DateTimeParser
            if (timeZone.equals(DEFAULT_TIME_ZONE)) {
                return JulianDateFormatParser.getInstance();
            }
            // Otherwise, create new DateTimeParser
            return new DateTimeParser() {
                private final DateTimeFormatter formatter = JULIAN_DATE_TIME_FORMATTER
                        .withZone(DateTimeZone.forTimeZone(timeZone));

                @Override
                public long parseDateTime(String dateTimeString) throws IllegalDataException {
                    try {
                        return formatter.parseDateTime(dateTimeString).getMillis();
                    } catch(IllegalArgumentException ex) {
                        throw new IllegalDataException(ex);
                    }
                }

                @Override
                public TimeZone getTimeZone() {
                    return timeZone;
                }
            };
        }
    }

    /**
     * This class is our default DateTime string parser
     */
    private static class JulianDateFormatParser implements DateTimeParser {
        private static final JulianDateFormatParser INSTANCE = new JulianDateFormatParser();

        public static JulianDateFormatParser getInstance() {
            return INSTANCE;
        }

        private final DateTimeFormatter formatter = JULIAN_DATE_TIME_FORMATTER.withZone(DateTimeZone.UTC);

        private JulianDateFormatParser() {}

        @Override
        public long parseDateTime(String dateTimeString) throws IllegalDataException {
            try {
                return formatter.parseDateTime(dateTimeString).getMillis();
            } catch(IllegalArgumentException ex) {
                throw new IllegalDataException(ex);
            }
        }

        @Override
        public TimeZone getTimeZone() {
            return formatter.getZone().toTimeZone();
        }
    }

    public static long rangeJodaHalfEven(DateTime roundedDT, DateTime otherDT, DateTimeFieldType type) {
        // It's OK if this is slow, as it's only called O(1) times per query
        //
        // We need to reverse engineer what roundHalfEvenCopy() does
        // and return the lower/upper (inclusive) range here
        // Joda simply works on milliseconds between the floor and ceil values.
        // We could avoid the period call for units less than a day, but this is not a perf
        // critical function.
        long roundedMs = roundedDT.getMillis();
        long otherMs = otherDT.getMillis();
        long midMs = (roundedMs + otherMs) / 2;
        long remainder = (roundedMs + otherMs) % 2;
        if (remainder == 0) {
            int roundedUnits = roundedDT.get(type);
            if (otherMs > roundedMs) {
             // Upper range, other is bigger.
                if ((roundedUnits & 1) == 0) {
                    // This unit is even, the next second is odd, so we get the mid point
                    return midMs;
                } else {
                    // This unit is odd, the next second is even and takes the midpoint.
                    return midMs -1;
                }
            } else {
                // Lower range, other is smaller.
                if ((roundedUnits & 1) == 0) {
                    // This unit is even, the next second is odd, so we get the mid point
                    return midMs;
                } else {
                    // This unit is odd, the next second is even and takes the midpoint.
                    return midMs + 1;
                }
            }
        } else {
            // probably never happens
            if (otherMs > roundedMs) {
                   // Upper range, return the rounded down value
                   return midMs;
               } else {
                   // Lower range, the mid value belongs to the previous unit.
                   return midMs + 1;
               }
        }
    }

    public static long rangeJodaHalfCeiling(DateTime roundedDT, DateTime otherDT, DateTimeFieldType type) {
        // It's OK if this is slow, as it's only called O(1) times per query
        //
        // We need to reverse engineer what roundHalfCeilingCopy() does
        // and return the lower/upper (inclusive) range here
        // Joda simply works on milliseconds between the floor and ceil values.
        // We could avoid the period call for units less than a day, but this is not a perf
        // critical function.
        long roundedMs = roundedDT.getMillis();
        long otherMs = otherDT.getMillis();
        long midMs = (roundedMs + otherMs) / 2;
        long remainder = (roundedMs + otherMs) % 2;
        if (remainder == 0) {
            if (otherMs > roundedMs) {
                    // The mid point to the bigger (other range)
                    return midMs -1;
            } else {
                    // The mid point goes to the bigger (our rounded range)
                    return midMs;
            }
        } else {
            // probably never happens
            if (otherMs > roundedMs) {
                   // Upper range, return the rounded down value
                   return midMs;
               } else {
                   // Lower range, the smaller value belongs to the previous unit.
                   return midMs + 1;
               }
        }
    }
}
