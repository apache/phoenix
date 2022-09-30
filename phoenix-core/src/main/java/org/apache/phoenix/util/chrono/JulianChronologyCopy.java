/*
 * Copyright (c) 2007-present, Stephen Colebourne & Michael Nascimento Santos
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 *  * Neither the name of JSR-310 nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.apache.phoenix.util.chrono;

import org.threeten.extra.chrono.JulianEra;

import java.io.Serializable;
import java.time.Clock;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.chrono.AbstractChronology;
import java.time.chrono.ChronoLocalDateTime;
import java.time.chrono.ChronoZonedDateTime;
import java.time.chrono.Chronology;
import java.time.chrono.Era;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.time.temporal.ValueRange;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The Julian calendar system.
 * <p>
 * This chronology defines the rules of the proleptic Julian calendar system.
 * This calendar system is the forerunner to the modern Gregorian and ISO calendars.
 * The Julian differs from the Gregorian only in terms of the leap year rule.
 * Dates are aligned such that {@code 0001-01-01 (Julian)} is {@code 0000-12-30 (ISO)}.
 * <p>
 * This class is proleptic. It implements Julian rules to the entire time-line.
 * <p>
 * This class implements a calendar where January 1st is the start of the year.
 * The history of the start of the year is complex and using the current standard
 * is the most consistent.
 * <p>
 * The fields are defined as follows:
 * <ul>
 * <li>era - There are two eras, the current 'Anno Domini' (AD) and the previous era 'Before Christ' (BC).
 * <li>year-of-era - The year-of-era for the current era increases uniformly from the epoch at year one.
 *  For the previous era the year increases from one as time goes backwards.
 * <li>proleptic-year - The proleptic year is the same as the year-of-era for the
 *  current era. For the previous era, years have zero, then negative values.
 * <li>month-of-year - There are 12 months in a Julian year, numbered from 1 to 12.
 * <li>day-of-month - There are between 28 and 31 days in each Julian month, numbered from 1 to 31.
 *  Months 4, 6, 9 and 11 have 30 days, Months 1, 3, 5, 7, 8, 10 and 12 have 31 days.
 *  Month 2 has 28 days, or 29 in a leap year.
 * <li>day-of-year - There are 365 days in a standard Julian year and 366 in a leap year.
 *  The days are numbered from 1 to 365 or 1 to 366.
 * <li>leap-year - Leap years occur every 4 years.
 * </ul>
 *
 * <h3>Implementation Requirements</h3>
 * This class is immutable and thread-safe.
 */
public final class JulianChronologyCopy extends AbstractChronology implements Serializable {

    /**
     * Singleton instance for the Julian chronology.
     */
    public static final JulianChronologyCopy INSTANCE = new JulianChronologyCopy();

    /**
     * Serialization version.
     */
    private static final long serialVersionUID = 7291205177830286973L;
    /**
     * Range of proleptic-year.
     */
    static final ValueRange YEAR_RANGE = ValueRange.of(-999_998, 999_999);
    /**
     * Range of year.
     */
    static final ValueRange YOE_RANGE = ValueRange.of(1, 999_999);
    /**
     * Range of proleptic month.
     */
    static final ValueRange PROLEPTIC_MONTH_RANGE = ValueRange.of(-999_998 * 12L, 999_999 * 12L + 11);

    /**
     * Private constructor, that is public to satisfy the {@code ServiceLoader}.
     * @deprecated Use the singleton {@link #INSTANCE} instead.
     */
    @Deprecated
    public JulianChronologyCopy() {
    }

    /**
     * Resolve singleton.
     *
     * @return the singleton instance, not null
     */
    private Object readResolve() {
        return INSTANCE;
    }

    //-----------------------------------------------------------------------
    /**
     * Gets the ID of the chronology - 'Julian'.
     * <p>
     * The ID uniquely identifies the {@code Chronology}.
     * It can be used to lookup the {@code Chronology} using {@link Chronology#of(String)}.
     *
     * @return the chronology ID - 'Julian'
     * @see #getCalendarType()
     */
    @Override
    public String getId() {
        return "Julian";
    }

    /**
     * Gets the calendar type of the underlying calendar system - 'julian'.
     * <p>
     * The <em>Unicode Locale Data Markup Language (LDML)</em> specification
     * does not define an identifier for the Julian calendar, but were it to
     * do so, 'julian' is highly likely to be chosen.
     *
     * @return the calendar system type - 'julian'
     * @see #getId()
     */
    @Override
    public String getCalendarType() {
        return "julian";
    }

    //-----------------------------------------------------------------------
    /**
     * Obtains a local date in Julian calendar system from the
     * era, year-of-era, month-of-year and day-of-month fields.
     *
     * @param era  the Julian era, not null
     * @param yearOfEra  the year-of-era
     * @param month  the month-of-year
     * @param dayOfMonth  the day-of-month
     * @return the Julian local date, not null
     * @throws DateTimeException if unable to create the date
     * @throws ClassCastException if the {@code era} is not a {@code JulianEra}
     */
    @Override
    public JulianDateCopy date(Era era, int yearOfEra, int month, int dayOfMonth) {
        return date(prolepticYear(era, yearOfEra), month, dayOfMonth);
    }

    /**
     * Obtains a local date in Julian calendar system from the
     * proleptic-year, month-of-year and day-of-month fields.
     *
     * @param prolepticYear  the proleptic-year
     * @param month  the month-of-year
     * @param dayOfMonth  the day-of-month
     * @return the Julian local date, not null
     * @throws DateTimeException if unable to create the date
     */
    @Override
    public JulianDateCopy date(int prolepticYear, int month, int dayOfMonth) {
        return JulianDateCopy.of(prolepticYear, month, dayOfMonth);
    }

    /**
     * Obtains a local date in Julian calendar system from the
     * era, year-of-era and day-of-year fields.
     *
     * @param era  the Julian era, not null
     * @param yearOfEra  the year-of-era
     * @param dayOfYear  the day-of-year
     * @return the Julian local date, not null
     * @throws DateTimeException if unable to create the date
     * @throws ClassCastException if the {@code era} is not a {@code JulianEra}
     */
    @Override
    public JulianDateCopy dateYearDay(Era era, int yearOfEra, int dayOfYear) {
        return dateYearDay(prolepticYear(era, yearOfEra), dayOfYear);
    }

    /**
     * Obtains a local date in Julian calendar system from the
     * proleptic-year and day-of-year fields.
     *
     * @param prolepticYear  the proleptic-year
     * @param dayOfYear  the day-of-year
     * @return the Julian local date, not null
     * @throws DateTimeException if unable to create the date
     */
    @Override
    public JulianDateCopy dateYearDay(int prolepticYear, int dayOfYear) {
        return JulianDateCopy.ofYearDay(prolepticYear, dayOfYear);
    }

    /**
     * Obtains a local date in the Julian calendar system from the epoch-day.
     *
     * @param epochDay  the epoch day
     * @return the Julian local date, not null
     * @throws DateTimeException if unable to create the date
     */
    @Override  // override with covariant return type
    public JulianDateCopy dateEpochDay(long epochDay) {
        return JulianDateCopy.ofEpochDay(epochDay);
    }

    //-------------------------------------------------------------------------
    /**
     * Obtains the current Julian local date from the system clock in the default time-zone.
     * <p>
     * This will query the {@link Clock#systemDefaultZone() system clock} in the default
     * time-zone to obtain the current date.
     * <p>
     * Using this method will prevent the ability to use an alternate clock for testing
     * because the clock is hard-coded.
     *
     * @return the current Julian local date using the system clock and default time-zone, not null
     * @throws DateTimeException if unable to create the date
     */
    @Override  // override with covariant return type
    public JulianDateCopy dateNow() {
        return JulianDateCopy.now();
    }

    /**
     * Obtains the current Julian local date from the system clock in the specified time-zone.
     * <p>
     * This will query the {@link Clock#system(ZoneId) system clock} to obtain the current date.
     * Specifying the time-zone avoids dependence on the default time-zone.
     * <p>
     * Using this method will prevent the ability to use an alternate clock for testing
     * because the clock is hard-coded.
     *
     * @param zone the zone ID to use, not null
     * @return the current Julian local date using the system clock, not null
     * @throws DateTimeException if unable to create the date
     */
    @Override  // override with covariant return type
    public JulianDateCopy dateNow(ZoneId zone) {
        return JulianDateCopy.now(zone);
    }

    /**
     * Obtains the current Julian local date from the specified clock.
     * <p>
     * This will query the specified clock to obtain the current date - today.
     * Using this method allows the use of an alternate clock for testing.
     * The alternate clock may be introduced using {@link Clock dependency injection}.
     *
     * @param clock  the clock to use, not null
     * @return the current Julian local date, not null
     * @throws DateTimeException if unable to create the date
     */
    @Override  // override with covariant return type
    public JulianDateCopy dateNow(Clock clock) {
        return JulianDateCopy.now(clock);
    }

    //-------------------------------------------------------------------------
    /**
     * Obtains a Julian local date from another date-time object.
     *
     * @param temporal  the date-time object to convert, not null
     * @return the Julian local date, not null
     * @throws DateTimeException if unable to create the date
     */
    @Override
    public JulianDateCopy date(TemporalAccessor temporal) {
        return JulianDateCopy.from(temporal);
    }

    /**
     * Obtains a Julian local date-time from another date-time object.
     *
     * @param temporal  the date-time object to convert, not null
     * @return the Julian local date-time, not null
     * @throws DateTimeException if unable to create the date-time
     */
    @Override
    @SuppressWarnings("unchecked")
    public ChronoLocalDateTime<JulianDateCopy> localDateTime(TemporalAccessor temporal) {
        return (ChronoLocalDateTime<JulianDateCopy>) super.localDateTime(temporal);
    }

    /**
     * Obtains a Julian zoned date-time from another date-time object.
     *
     * @param temporal  the date-time object to convert, not null
     * @return the Julian zoned date-time, not null
     * @throws DateTimeException if unable to create the date-time
     */
    @Override
    @SuppressWarnings("unchecked")
    public ChronoZonedDateTime<JulianDateCopy> zonedDateTime(TemporalAccessor temporal) {
        return (ChronoZonedDateTime<JulianDateCopy>) super.zonedDateTime(temporal);
    }

    /**
     * Obtains a Julian zoned date-time in this chronology from an {@code Instant}.
     *
     * @param instant  the instant to create the date-time from, not null
     * @param zone  the time-zone, not null
     * @return the Julian zoned date-time, not null
     * @throws DateTimeException if the result exceeds the supported range
     */
    @Override
    @SuppressWarnings("unchecked")
    public ChronoZonedDateTime<JulianDateCopy> zonedDateTime(Instant instant, ZoneId zone) {
        return (ChronoZonedDateTime<JulianDateCopy>) super.zonedDateTime(instant, zone);
    }

    //-----------------------------------------------------------------------
    /**
     * Checks if the specified year is a leap year.
     * <p>
     * A Julian proleptic-year is leap if the remainder after division by four equals zero.
     * This method does not validate the year passed in, and only has a
     * well-defined result for years in the supported range.
     *
     * @param prolepticYear  the proleptic-year to check, not validated for range
     * @return true if the year is a leap year
     */
    @Override
    public boolean isLeapYear(long prolepticYear) {
        return (prolepticYear % 4) == 0;
    }

    @Override
    public int prolepticYear(Era era, int yearOfEra) {
        if (era instanceof JulianEra == false) {
            throw new ClassCastException("Era must be JulianEra");
        }
        return (era == JulianEra.AD ? yearOfEra : 1 - yearOfEra);
    }

    @Override
    public JulianEra eraOf(int eraValue) {
        return JulianEra.of(eraValue);
    }

    @Override
    public List<Era> eras() {
        return Arrays.<Era>asList(JulianEra.values());
    }

    //-----------------------------------------------------------------------
    @Override
    public ValueRange range(ChronoField field) {
        switch (field) {
            case PROLEPTIC_MONTH:
                return PROLEPTIC_MONTH_RANGE;
            case YEAR_OF_ERA:
                return YOE_RANGE;
            case YEAR:
                return YEAR_RANGE;
            default:
                break;
        }
        return field.range();
    }

    //-----------------------------------------------------------------------
    @Override  // override for return type
    public JulianDateCopy resolveDate(Map<TemporalField, Long> fieldValues, ResolverStyle resolverStyle) {
        return (JulianDateCopy) super.resolveDate(fieldValues, resolverStyle);
    }

}
