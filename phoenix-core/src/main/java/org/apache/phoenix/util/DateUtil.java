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



@SuppressWarnings("serial")
public class DateUtil {
    public static final TimeZone DATE_TIME_ZONE = TimeZone.getTimeZone("GMT");
    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"; // This is the format the app sets in NLS settings for every connection.
    public static final Format DEFAULT_DATE_FORMATTER = FastDateFormat.getInstance(DEFAULT_DATE_FORMAT, DATE_TIME_ZONE);

    public static final String DEFAULT_MS_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final Format DEFAULT_MS_DATE_FORMATTER = FastDateFormat.getInstance(DEFAULT_MS_DATE_FORMAT, DATE_TIME_ZONE);

    private DateUtil() {
    }

    public static Format getDateParser(String pattern) {
        SimpleDateFormat format = new SimpleDateFormat(pattern) {
            @Override
            public java.util.Date parseObject(String source) throws ParseException {
                java.util.Date date = super.parse(source);
                return new java.sql.Date(date.getTime());
            }
        };
        format.setTimeZone(DateUtil.DATE_TIME_ZONE);
        return format;
    }

    public static Format getTimeParser(String pattern) {
        SimpleDateFormat format = new SimpleDateFormat(pattern) {
            @Override
            public java.util.Date parseObject(String source) throws ParseException {
                java.util.Date date = super.parse(source);
                return new java.sql.Time(date.getTime());
            }
        };
        format.setTimeZone(DateUtil.DATE_TIME_ZONE);
        return format;
    }

    public static Format getTimestampParser(String pattern) {
        SimpleDateFormat format = new SimpleDateFormat(pattern) {
            @Override
            public java.util.Date parseObject(String source) throws ParseException {
                java.util.Date date = super.parse(source);
                return new java.sql.Timestamp(date.getTime());
            }
        };
        format.setTimeZone(DateUtil.DATE_TIME_ZONE);
        return format;
    }

    public static Format getDateFormatter(String pattern) {
        return DateUtil.DEFAULT_DATE_FORMAT.equals(pattern) ? DateUtil.DEFAULT_DATE_FORMATTER : FastDateFormat.getInstance(pattern, DateUtil.DATE_TIME_ZONE);
    }

    private static ThreadLocal<Format> dateFormat =
            new ThreadLocal < Format > () {
        @Override protected Format initialValue() {
            return getDateParser(DEFAULT_DATE_FORMAT);
        }
    };

    public static Date parseDate(String dateValue) {
        try {
            return (Date)dateFormat.get().parseObject(dateValue);
        } catch (ParseException e) {
            throw new IllegalDataException(e);
        }
    }

    private static ThreadLocal<Format> timeFormat =
            new ThreadLocal < Format > () {
        @Override protected Format initialValue() {
            return getTimeParser(DEFAULT_DATE_FORMAT);
        }
    };

    public static Time parseTime(String timeValue) {
        try {
            return (Time)timeFormat.get().parseObject(timeValue);
        } catch (ParseException e) {
            throw new IllegalDataException(e);
        }
    }

    private static ThreadLocal<Format> timestampFormat =
            new ThreadLocal < Format > () {
        @Override protected Format initialValue() {
            return getTimestampParser(DEFAULT_DATE_FORMAT);
        }
    };

    public static Timestamp parseTimestamp(String timeValue) {
        try {
            return (Timestamp)timestampFormat.get().parseObject(timeValue);
        } catch (ParseException e) {
            throw new IllegalDataException(e);
        }
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
}
