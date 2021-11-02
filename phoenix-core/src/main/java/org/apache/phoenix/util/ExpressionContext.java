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
import java.util.Properties;

public interface ExpressionContext {

    public Date parseDate(String dateValue);

    public Time parseTime(String timeValue);

    public Timestamp parseTimestamp(String timestampValue);

    public Format getTemporalFormatter(String pattern);

    //FIXME eliminate, encapsulate the formatting here instead
    public Format getDateFormatter();

    //FIXME eliminate, encapsulate the formatting here instead
    public Format getTimeFormatter();

    //FIXME eliminate, encapsulate the formatting here instead
    public String getTimestampFormatPattern();
    
    public String getDateFormatPattern();

    public String getTimeFormatPattern();

    public Format getTimestampFormatter();

    public String resolveTimezoneId(String timezoneId);

    public String getTimezoneId();

    boolean isCompliant();

    public Properties toProps();

    //Context dependent function implementations
    public int secondImplementation(long epochMs);
    
    public int minuteImplementation(long epochMs);
    
    public int hourImplementation(long epochMs);

    public int dayOfMonthImplementation(long epochMs);
    
    public int dayOfWeekImplementation(long epochMs);
    
    public int dayOfYearImplementation(long epochMs);
    
    public int yearImplementation(long epochMs);
    
    public int weekImplementation(long epochMs);

    public int monthImplementation(long epochMs);

    public long roundSecond(long epochMs, long divBy, long multiplier);

    public long roundMinute(long epochMs, long divBy, long multiplier);

    public long roundHour(long epochMs, long divBy, long multiplier);

    public long roundDay(long epochMs, long divBy, long multiplier);

    public long roundWeek(long epochMs, long divBy, long multiplier);

    public long roundMonth(long epochMs, long divBy, long multiplier);

    public long roundYear(long epochMs, long divBy, long multiplier);

    public long floorSecond(long epochMs, long divBy, long multiplier);

    public long floorMinute(long epochMs, long divBy, long multiplier);

    public long floorHour(long epochMs, long divBy, long multiplier);

    public long floorDay(long epochMs, long divBy, long multiplier);

    public long floorWeek(long epochMs, long divBy, long multiplier);

    public long floorMonth(long epochMs, long divBy, long multiplier);

    public long floorYear(long epochMs, long divBy, long multiplier);

    public long ceilSecond(long epochMs, long divBy, long multiplier);

    public long ceilMinute(long epochMs, long divBy, long multiplier);

    public long ceilHour(long epochMs, long divBy, long multiplier);

    public long ceilDay(long epochMs, long divBy, long multiplier);

    public long ceilWeek(long epochMs, long divBy, long multiplier);

    public long ceilMonth(long epochMs, long divBy, long multiplier);

    public long ceilYear(long epochMs, long divBy, long multiplier);

    public long roundSecondUpper(long epochMs, long divBy, long multiplier);

    public long roundMinuteUpper(long epochMs, long divBy, long multiplier);

    public long roundHourUpper(long epochMs, long divBy, long multiplier);

    public long roundDayUpper(long epochMs, long divBy, long multiplier);

    public long roundWeekUpper(long epochMs, long divBy, long multiplier);

    public long roundMonthUpper(long epochMs, long divBy, long multiplier);

    public long roundYearUpper(long epochMs, long divBy, long multiplier);

    public long roundSecondLower(long epochMs, long divBy, long multiplier);

    public long roundMinuteLower(long epochMs, long divBy, long multiplier);

    public long roundHourLower(long epochMs, long divBy, long multiplier);

    public long roundDayLower(long epochMs, long divBy, long multiplier);

    public long roundWeekLower(long epochMs, long divBy, long multiplier);

    public long roundMonthLower(long epochMs, long divBy, long multiplier);

    public long roundYearLower(long epochMs, long divBy, long multiplier);

}
