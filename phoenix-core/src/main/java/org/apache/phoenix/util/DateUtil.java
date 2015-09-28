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

import static org.apache.phoenix.query.QueryConstants.MAX_ALLOWED_NANOS;
import static org.apache.phoenix.query.QueryConstants.MILLIS_TO_NANOS_CONVERTOR;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.types.PDataType;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.ISODateTimeFormat;

import com.google.common.collect.Lists;


@SuppressWarnings({ "serial", "deprecation" })
public class DateUtil {
    public static final String DEFAULT_TIME_ZONE_ID = "GMT";
    public static final String LOCAL_TIME_ZONE_ID = "LOCAL";
    private static final TimeZone DEFAULT_TIME_ZONE = TimeZone.getTimeZone(DEFAULT_TIME_ZONE_ID);
    
    public static final String DEFAULT_MS_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final Format DEFAULT_MS_DATE_FORMATTER = FastDateFormat.getInstance(
            DEFAULT_MS_DATE_FORMAT, TimeZone.getTimeZone(DEFAULT_TIME_ZONE_ID));

    public static final String DEFAULT_DATE_FORMAT = DEFAULT_MS_DATE_FORMAT;
    public static final Format DEFAULT_DATE_FORMATTER = DEFAULT_MS_DATE_FORMATTER;

    public static final String DEFAULT_TIME_FORMAT = DEFAULT_MS_DATE_FORMAT;
    public static final Format DEFAULT_TIME_FORMATTER = DEFAULT_MS_DATE_FORMATTER;

    public static final String DEFAULT_TIMESTAMP_FORMAT = DEFAULT_MS_DATE_FORMAT;
    public static final Format DEFAULT_TIMESTAMP_FORMATTER = DEFAULT_MS_DATE_FORMATTER;

    private static final DateTimeFormatter ISO_DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
        .append(ISODateTimeFormat.dateParser())
        .appendOptional(new DateTimeFormatterBuilder()
                .appendLiteral(' ').toParser())
        .appendOptional(new DateTimeFormatterBuilder()
                .append(ISODateTimeFormat.timeParser()).toParser())
        .toFormatter().withChronology(ISOChronology.getInstanceUTC());
    
    private DateUtil() {
    }

    private static TimeZone getTimeZone(String timeZoneId) {
        TimeZone parserTimeZone;
        if (timeZoneId == null) {
            parserTimeZone = DateUtil.DEFAULT_TIME_ZONE;
        } else if (LOCAL_TIME_ZONE_ID.equalsIgnoreCase(timeZoneId)) {
            parserTimeZone = TimeZone.getDefault();
        } else {
            parserTimeZone = TimeZone.getTimeZone(timeZoneId);
        }
        return parserTimeZone;
    }
    
    private static String[] defaultPattern;
    static {
        int maxOrdinal = Integer.MIN_VALUE;
        List<PDataType> timeDataTypes = Lists.newArrayListWithExpectedSize(6);
        for (PDataType type : PDataType.values()) {
            if (java.util.Date.class.isAssignableFrom(type.getJavaClass())) {
                timeDataTypes.add(type);
                if (type.ordinal() > maxOrdinal) {
                    maxOrdinal = type.ordinal();
                }
            }
        }
        defaultPattern = new String[maxOrdinal+1];
        for (PDataType type : timeDataTypes) {
            switch (type.getResultSetSqlType()) {
            case Types.TIMESTAMP:
                defaultPattern[type.ordinal()] = DateUtil.DEFAULT_TIMESTAMP_FORMAT;
                break;
            case Types.TIME:
                defaultPattern[type.ordinal()] = DateUtil.DEFAULT_TIME_FORMAT;
                break;
            case Types.DATE:
                defaultPattern[type.ordinal()] = DateUtil.DEFAULT_DATE_FORMAT;
                break;
            }
        }
    }
    
    private static String getDefaultFormat(PDataType type) {
        int ordinal = type.ordinal();
        if (ordinal >= 0 || ordinal < defaultPattern.length) {
            String format = defaultPattern[ordinal];
            if (format != null) {
                return format;
            }
        }
        throw new IllegalArgumentException("Expected a date/time type, but got " + type);
    }

    public static DateTimeParser getDateTimeParser(String pattern, PDataType pDataType, String timeZoneId) {
        TimeZone timeZone = getTimeZone(timeZoneId);
        String defaultPattern = getDefaultFormat(pDataType);
        if (pattern == null || pattern.length() == 0) {
            pattern = defaultPattern;
        }
        if(defaultPattern.equals(pattern)) {
            return ISODateFormatParserFactory.getParser(timeZone);
        } else {
            return new SimpleDateFormatParser(pattern, timeZone);
        }
    }

    public static DateTimeParser getDateTimeParser(String pattern, PDataType pDataType) {
        return getDateTimeParser(pattern, pDataType, null);
    }

    public static Format getDateFormatter(String pattern) {
        return DateUtil.DEFAULT_DATE_FORMAT.equals(pattern)
                ? DateUtil.DEFAULT_DATE_FORMATTER
                : FastDateFormat.getInstance(pattern, DateUtil.DEFAULT_TIME_ZONE);
    }

    public static Format getTimeFormatter(String pattern) {
        return DateUtil.DEFAULT_TIME_FORMAT.equals(pattern)
                ? DateUtil.DEFAULT_TIME_FORMATTER
                : FastDateFormat.getInstance(pattern, DateUtil.DEFAULT_TIME_ZONE);
    }

    public static Format getTimestampFormatter(String pattern) {
        return DateUtil.DEFAULT_TIMESTAMP_FORMAT.equals(pattern)
                ? DateUtil.DEFAULT_TIMESTAMP_FORMATTER
                : FastDateFormat.getInstance(pattern, DateUtil.DEFAULT_TIME_ZONE);
    }

    private static long parseDateTime(String dateTimeValue) {
        return ISODateFormatParser.getInstance().parseDateTime(dateTimeValue);
    }

    public static Date parseDate(String dateValue) {
        return new Date(parseDateTime(dateValue));
    }

    public static Time parseTime(String timeValue) {
        return new Time(parseDateTime(timeValue));
    }

    public static Timestamp parseTimestamp(String timestampValue) {
        return new Timestamp(parseDateTime(timestampValue));
    }

    /**
     * Utility function to work around the weirdness of the {@link Timestamp} constructor.
     * This method takes the milli-seconds that spills over to the nanos part as part of 
     * constructing the {@link Timestamp} object.
     * If we just set the nanos part of timestamp to the nanos passed in param, we 
     * end up losing the sub-second part of timestamp. 
     */
    public static Timestamp getTimestamp(long millis, int nanos) {
        if (nanos > MAX_ALLOWED_NANOS || nanos < 0) {
            throw new IllegalArgumentException("nanos > " + MAX_ALLOWED_NANOS + " or < 0");
        }
        Timestamp ts = new Timestamp(millis);
        if (ts.getNanos() + nanos > MAX_ALLOWED_NANOS) {
            int millisToNanosConvertor = BigDecimal.valueOf(MILLIS_TO_NANOS_CONVERTOR).intValue();
            int overFlowMs = (ts.getNanos() + nanos) / millisToNanosConvertor;
            int overFlowNanos = (ts.getNanos() + nanos) - (overFlowMs * millisToNanosConvertor);
            ts = new Timestamp(millis + overFlowMs);
            ts.setNanos(ts.getNanos() + overFlowNanos);
        } else {
            ts.setNanos(ts.getNanos() + nanos);
        }
        return ts;
    }

    /**
     * Utility function to convert a {@link BigDecimal} value to {@link Timestamp}.
     */
    public static Timestamp getTimestamp(BigDecimal bd) {
        return DateUtil.getTimestamp(bd.longValue(), ((bd.remainder(BigDecimal.ONE).multiply(BigDecimal.valueOf(MILLIS_TO_NANOS_CONVERTOR))).intValue()));
    }

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
                java.util.Date date =parser.parse(dateTimeString);
                return date.getTime();
            } catch (ParseException e) {
                throw new IllegalDataException("Unable to parse date/time '" + dateTimeString + "' using format string of '" + datePattern + "'.");
            }
        }

        @Override
        public TimeZone getTimeZone() {
            return parser.getTimeZone();
        }
    }

    private static class ISODateFormatParserFactory {
        private ISODateFormatParserFactory() {}
        
        public static DateTimeParser getParser(final TimeZone timeZone) {
            // If timeZone matches default, get singleton DateTimeParser
            if (timeZone.equals(DEFAULT_TIME_ZONE)) {
                return ISODateFormatParser.getInstance();
            }
            // Otherwise, create new DateTimeParser
            return new DateTimeParser() {
                private final DateTimeFormatter formatter = ISO_DATE_TIME_FORMATTER
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
    private static class ISODateFormatParser implements DateTimeParser {
        private static final ISODateFormatParser INSTANCE = new ISODateFormatParser();

        public static ISODateFormatParser getInstance() {
            return INSTANCE;
        }

        private final DateTimeFormatter formatter = ISO_DATE_TIME_FORMATTER.withZoneUTC();

        private ISODateFormatParser() {}

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
}
