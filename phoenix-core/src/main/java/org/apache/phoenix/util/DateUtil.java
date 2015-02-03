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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.IllegalDataException;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.ISODateTimeFormat;


@SuppressWarnings("serial")
public class DateUtil {
    public static final String DEFAULT_TIME_ZONE_ID = "GMT";
    private static final TimeZone DEFAULT_TIME_ZONE = TimeZone.getTimeZone(DEFAULT_TIME_ZONE_ID);
    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"; // This is the format the app sets in NLS settings for every connection.
    public static final Format DEFAULT_DATE_FORMATTER = FastDateFormat.getInstance(
            DEFAULT_DATE_FORMAT, TimeZone.getTimeZone(DEFAULT_TIME_ZONE_ID));

    public static final String DEFAULT_MS_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final Format DEFAULT_MS_DATE_FORMATTER = FastDateFormat.getInstance(
            DEFAULT_MS_DATE_FORMAT, TimeZone.getTimeZone(DEFAULT_TIME_ZONE_ID));

    private static final DateTimeFormatter ISO_DATE_TIME_PARSER = new DateTimeFormatterBuilder()
            .append(ISODateTimeFormat.dateParser())
            .appendOptional(new DateTimeFormatterBuilder()
                    .appendLiteral(' ').toParser())
            .appendOptional(new DateTimeFormatterBuilder()
                    .append(ISODateTimeFormat.timeParser()).toParser())
            .toFormatter()
            .withZoneUTC()
            .withChronology(ISOChronology.getInstanceUTC());

    private DateUtil() {
    }

    public static DateTimeParser getDateParser(String pattern, TimeZone timeZone) {
        if(DateUtil.DEFAULT_DATE_FORMAT.equals(pattern) &&
                timeZone.getID().equalsIgnoreCase(DateUtil.DEFAULT_TIME_ZONE_ID)) {
            return ISODateFormatParser.getInstance();
        } else {
            return new SimpleDateFormatParser(pattern, timeZone);
        }
    }

    public static DateTimeParser getDateParser(String pattern, String timeZoneId) {
        if(timeZoneId == null) {
            timeZoneId = DateUtil.DEFAULT_TIME_ZONE_ID;
        }
        TimeZone parserTimeZone;
        if ("LOCAL".equalsIgnoreCase(timeZoneId)) {
            parserTimeZone = TimeZone.getDefault();
        } else {
            parserTimeZone = TimeZone.getTimeZone(timeZoneId);
        }
        return getDateParser(pattern, parserTimeZone);
    }

    public static DateTimeParser getDateParser(String pattern) {
        return getDateParser(pattern, DEFAULT_TIME_ZONE);
    }

    public static DateTimeParser getTimeParser(String pattern, TimeZone timeZone) {
        return getDateParser(pattern, timeZone);
    }

    public static DateTimeParser getTimeParser(String pattern) {
        return getTimeParser(pattern, DEFAULT_TIME_ZONE);
    }

    public static DateTimeParser getTimestampParser(String pattern, TimeZone timeZone) {
        return getDateParser(pattern, timeZone);
    }

    public static DateTimeParser getTimestampParser(String pattern) {
        return getTimestampParser(pattern, DEFAULT_TIME_ZONE);
    }

    public static Format getDateFormatter(String pattern) {
        return DateUtil.DEFAULT_DATE_FORMAT.equals(pattern)
                ? DateUtil.DEFAULT_DATE_FORMATTER
                : FastDateFormat.getInstance(pattern, DateUtil.DEFAULT_TIME_ZONE);
    }

    public static Date parseDateTime(String dateTimeValue) {
        return ISODateFormatParser.getInstance().parseDateTime(dateTimeValue);
    }

    public static Date parseDate(String dateValue) {
        return parseDateTime(dateValue);
    }

    public static Time parseTime(String timeValue) {
        return new Time(parseDateTime(timeValue).getTime());
    }

    public static Timestamp parseTimestamp(String timestampValue) {
        return new Timestamp(parseDateTime(timestampValue).getTime());
    }

    /**
     * Utility function to work around the weirdness of the {@link Timestamp} constructor.
     * This method takes the milli-seconds that spills over to the nanos part as part of 
     * constructing the {@link Timestamp} object.
     * If we just set the nanos part of timestamp to the nanos passed in param, we 
     * end up losing the sub-second part of timestamp. 
     */
    public static Timestamp getTimestamp(long millis, int nanos) {
        Timestamp ts = new Timestamp(millis);
        ts.setNanos(ts.getNanos() + nanos);
        return ts;
    }

    /**
     * Utility function to convert a {@link BigDecimal} value to {@link Timestamp}.
     */
    public static Timestamp getTimestamp(BigDecimal bd) {
        return DateUtil.getTimestamp(bd.longValue(), ((bd.remainder(BigDecimal.ONE).multiply(BigDecimal.valueOf(QueryConstants.MILLIS_TO_NANOS_CONVERTOR))).intValue()));
    }

    public static interface DateTimeParser {
        public Date parseDateTime(String dateTimeString) throws IllegalDataException;
    }

    /**
     * This class is used when a user explicitly provides phoenix.query.dateFormat in configuration
     */
    private static class SimpleDateFormatParser implements DateTimeParser {
        private String datePattern;
        private SimpleDateFormat parser;

        public SimpleDateFormatParser(String pattern) {
            this(pattern, DEFAULT_TIME_ZONE);
        }

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

        public Date parseDateTime(String dateTimeString) throws IllegalDataException {
            try {
                java.util.Date date =parser.parse(dateTimeString);
                return new java.sql.Date(date.getTime());
            } catch (ParseException e) {
                throw new IllegalDataException("to_date('" + dateTimeString + "') did not match expected date format of '" + datePattern + "'.");
            }
        }
    }

    /**
     * This class is our default DateTime string parser
     */
    private static class ISODateFormatParser implements DateTimeParser {
        private static ISODateFormatParser inst = null;
        private static Object lock = new Object();
        private ISODateFormatParser() {}

        public static ISODateFormatParser getInstance() {
            if(inst != null) return inst;

            synchronized (lock) {
                if (inst == null) {
                    inst = new ISODateFormatParser();
                }
            }
            return inst;
        }

        public Date parseDateTime(String dateTimeString) throws IllegalDataException {
            try {
                return new Date(ISO_DATE_TIME_PARSER.parseDateTime(dateTimeString).getMillis());
            } catch(IllegalArgumentException ex) {
                throw new IllegalDataException(ex);
            }
        }
    }
}
