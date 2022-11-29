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

import org.threeten.extra.chrono.JulianChronology;
import org.threeten.extra.chrono.JulianEra;

import java.io.Serializable;
import java.time.Clock;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.chrono.AbstractChronology;
import java.time.chrono.ChronoLocalDateTime;
import java.time.chrono.ChronoZonedDateTime;
import java.time.chrono.Chronology;
import java.time.chrono.Era;
import java.time.chrono.IsoChronology;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.time.temporal.ValueRange;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The GregorianJulian calendar system.
 * <p>
 * The GregorianJulian calendar system follows the rules of the Julian calendar
 * until 1582 and the rules of the Gregorian (ISO) calendar since then.
 * The Julian differs from the Gregorian only in terms of the leap year rule.
 * <p>
 * The Julian and Gregorian calendar systems are linked to Rome and the Vatican
 * with the Julian preceding the Gregorian. The Gregorian was introduced to
 * handle the drift of the seasons through the year due to the inaccurate
 * Julian leap year rules. When first introduced by the Vatican in 1582,
 * the cutover resulted in a "gap" of 10 days.
 * <p>
 * This chronology implements the proleptic Julian calendar system followed by
 * the proleptic Gregorian calendar system (identical to the ISO calendar system).
 * <p>
 * This class implements a calendar where January 1st is the start of the year.
 * The history of the start of the year is complex and using the current standard
 * is the most consistent.
 * <p>
 * The eras of this calendar system are defined by {@link JulianEra} to avoid unnecessary duplication.
 * <p>
 * The fields are defined as follows:
 * <ul>
 * <li>era - There are two eras, the current 'Anno Domini' (AD) and the previous era 'Before Christ' (BC).
 * <li>year-of-era - The year-of-era for the current era increases uniformly from the epoch at year one.
 *  For the previous era the year increases from one as time goes backwards.
 * <li>proleptic-year - The proleptic year is the same as the year-of-era for the
 *  current era. For the previous era, years have zero, then negative values.
 * <li>month-of-year - There are 12 months in a year, numbered from 1 to 12.
 * <li>day-of-month - There are between 28 and 31 days in each month, numbered from 1 to 31.
 *  Months 4, 6, 9 and 11 have 30 days, Months 1, 3, 5, 7, 8, 10 and 12 have 31 days.
 *  Month 2 has 28 days, or 29 in a leap year.
 *  The cutover October 4, 1582 (Julian) is followed by October 15, 1582 (Gregorian)
 * <li>day-of-year - There are 365 days in a standard year and 366 in a leap year.
 *  The days are numbered from 1 to 365 or 1 to 366.
 *  The cutover year 1582 has values from 1 to 356 and a length of 356 days.
 * </ul>
 *
 * <h3>Implementation Requirements</h3>
 * This class is immutable and thread-safe.
 */
public final class GJChronology
        extends AbstractChronology
        implements Serializable {

    /**
     * Singleton instance for the Coptic chronology.
     */
    public static final GJChronology INSTANCE = new GJChronology();
    /**
     * The cutover date, Friday 15th October 1582.
     */
    public static final LocalDate CUTOVER = LocalDate.of(1582, 10, 15);
    /**
     * The number of cutover days.
     */
    static final int CUTOVER_DAYS = 10;
    /**
     * The cutover year.
     */
    static final int CUTOVER_YEAR = 1582;

    /**
     * Serialization version.
     */
    private static final long serialVersionUID = 87235724675472658L;
    /**
     * Range of day-of-year.
     */
    static final ValueRange DOY_RANGE = ValueRange.of(1, 356, 366);
    /**
     * Range of aligned-week-of-month.
     */
    static final ValueRange ALIGNED_WOM_RANGE = ValueRange.of(1, 3, 5);
    /**
     * Range of aligned-week-of-year.
     */
    static final ValueRange ALIGNED_WOY_RANGE = ValueRange.of(1, 51, 53);
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
    public GJChronology() {
    }

    /**
     * Resolve singleton.
     *
     * @return the singleton instance, not null
     */
    private Object readResolve() {
        return INSTANCE;
    }

    //-------------------------------------------------------------------------
    /**
     * Gets the cutover date between the Julian and Gregorian calendar.
     * <p>
     * The date returned is the first date that the Gregorian (ISO) calendar applies,
     * which is Friday 15th October 1582.
     *
     * @return the first date after the cutover, not null
     */
    public LocalDate getCutover() {
        return CUTOVER;
    }

    //-----------------------------------------------------------------------
    /**
     * Gets the ID of the chronology - 'GregorianJulian'.
     * <p>
     * The ID uniquely identifies the {@code Chronology}.
     * It can be used to lookup the {@code Chronology} using {@link Chronology#of(String)}.
     *
     * @return the chronology ID - 'GregorianJulian'
     * @see #getCalendarType()
     */
    @Override
    public String getId() {
        return "GregorianJulian";
    }

    /**
     * Gets the calendar type of the underlying calendar system, which returns null.
     * <p>
     * The <em>Unicode Locale Data Markup Language (LDML)</em> specification
     * does not define an identifier for this calendar system, thus null is returned.
     *
     * @return the calendar system type, null
     * @see #getId()
     */
    @Override
    public String getCalendarType() {
        return null;
    }

    //-----------------------------------------------------------------------
    /**
     * Obtains a local date in GregorianJulian calendar system from the
     * era, year-of-era, month-of-year and day-of-month fields.
     * <p>
     * Dates in the middle of the cutover gap, such as the 10th October 1582,
     * will not throw an exception. Instead, the date will be treated as a Julian date
     * and converted to an ISO date, with the day of month shifted by 10 days.
     *
     * @param era  the GregorianJulian era, not null
     * @param yearOfEra  the year-of-era
     * @param month  the month-of-year
     * @param dayOfMonth  the day-of-month
     * @return the GregorianJulian local date, not null
     * @throws DateTimeException if unable to create the date
     * @throws ClassCastException if the {@code era} is not a {@code JulianEra}
     */
    @Override
    public GJDate date(Era era, int yearOfEra, int month, int dayOfMonth) {
        return date(prolepticYear(era, yearOfEra), month, dayOfMonth);
    }

    /**
     * Obtains a local date in GregorianJulian calendar system from the
     * proleptic-year, month-of-year and day-of-month fields.
     * <p>
     * Dates in the middle of the cutover gap, such as the 10th October 1582,
     * will not throw an exception. Instead, the date will be treated as a Julian date
     * and converted to an ISO date, with the day of month shifted by 10 days.
     *
     * @param prolepticYear  the proleptic-year
     * @param month  the month-of-year
     * @param dayOfMonth  the day-of-month
     * @return the GregorianJulian local date, not null
     * @throws DateTimeException if unable to create the date
     */
    @Override
    public GJDate date(int prolepticYear, int month, int dayOfMonth) {
        return GJDate.of(prolepticYear, month, dayOfMonth);
    }

    /**
     * Obtains a local date in GregorianJulian calendar system from the
     * era, year-of-era and day-of-year fields.
     * <p>
     * The day-of-year takes into account the cutover, thus there are only 356 days in 1582.
     *
     * @param era  the GregorianJulian era, not null
     * @param yearOfEra  the year-of-era
     * @param dayOfYear  the day-of-year
     * @return the GregorianJulian local date, not null
     * @throws DateTimeException if unable to create the date
     * @throws ClassCastException if the {@code era} is not a {@code JulianEra}
     */
    @Override
    public GJDate dateYearDay(Era era, int yearOfEra, int dayOfYear) {
        return dateYearDay(prolepticYear(era, yearOfEra), dayOfYear);
    }

    /**
     * Obtains a local date in GregorianJulian calendar system from the
     * proleptic-year and day-of-year fields.
     * <p>
     * The day-of-year takes into account the cutover, thus there are only 356 days in 1582.
     *
     * @param prolepticYear  the proleptic-year
     * @param dayOfYear  the day-of-year
     * @return the GregorianJulian local date, not null
     * @throws DateTimeException if unable to create the date
     */
    @Override
    public GJDate dateYearDay(int prolepticYear, int dayOfYear) {
        return GJDate.ofYearDay(prolepticYear, dayOfYear);
    }

    /**
     * Obtains a local date in the GregorianJulian calendar system from the epoch-day.
     *
     * @param epochDay  the epoch day
     * @return the GregorianJulian local date, not null
     * @throws DateTimeException if unable to create the date
     */
    @Override  // override with covariant return type
    public GJDate dateEpochDay(long epochDay) {
        return GJDate.ofEpochDay(epochDay);
    }

    //-------------------------------------------------------------------------
    /**
     * Obtains the current GregorianJulian local date from the system clock in the default time-zone.
     * <p>
     * This will query the {@link Clock#systemDefaultZone() system clock} in the default
     * time-zone to obtain the current date.
     * <p>
     * Using this method will prevent the ability to use an alternate clock for testing
     * because the clock is hard-coded.
     *
     * @return the current GregorianJulian local date using the system clock and default time-zone, not null
     * @throws DateTimeException if unable to create the date
     */
    @Override  // override with covariant return type
    public GJDate dateNow() {
        return GJDate.now();
    }

    /**
     * Obtains the current GregorianJulian local date from the system clock in the specified time-zone.
     * <p>
     * This will query the {@link Clock#system(ZoneId) system clock} to obtain the current date.
     * Specifying the time-zone avoids dependence on the default time-zone.
     * <p>
     * Using this method will prevent the ability to use an alternate clock for testing
     * because the clock is hard-coded.
     *
     * @param zone the zone ID to use, not null
     * @return the current GregorianJulian local date using the system clock, not null
     * @throws DateTimeException if unable to create the date
     */
    @Override  // override with covariant return type
    public GJDate dateNow(ZoneId zone) {
        return GJDate.now(zone);
    }

    /**
     * Obtains the current GregorianJulian local date from the specified clock.
     * <p>
     * This will query the specified clock to obtain the current date - today.
     * Using this method allows the use of an alternate clock for testing.
     * The alternate clock may be introduced using {@link Clock dependency injection}.
     *
     * @param clock  the clock to use, not null
     * @return the current GregorianJulian local date, not null
     * @throws DateTimeException if unable to create the date
     */
    @Override  // override with covariant return type
    public GJDate dateNow(Clock clock) {
        return GJDate.now(clock);
    }

    //-------------------------------------------------------------------------
    /**
     * Obtains a GregorianJulian local date from another date-time object.
     *
     * @param temporal  the date-time object to convert, not null
     * @return the GregorianJulian local date, not null
     * @throws DateTimeException if unable to create the date
     */
    @Override
    public GJDate date(TemporalAccessor temporal) {
        return GJDate.from(temporal);
    }

    /**
     * Obtains a GregorianJulian local date-time from another date-time object.
     *
     * @param temporal  the date-time object to convert, not null
     * @return the GregorianJulian local date-time, not null
     * @throws DateTimeException if unable to create the date-time
     */
    @Override
    @SuppressWarnings("unchecked")
    public ChronoLocalDateTime<GJDate> localDateTime(TemporalAccessor temporal) {
        return (ChronoLocalDateTime<GJDate>) super.localDateTime(temporal);
    }

    /**
     * Obtains a GregorianJulian zoned date-time from another date-time object.
     *
     * @param temporal  the date-time object to convert, not null
     * @return the GregorianJulian zoned date-time, not null
     * @throws DateTimeException if unable to create the date-time
     */
    @Override
    @SuppressWarnings("unchecked")
    public ChronoZonedDateTime<GJDate> zonedDateTime(TemporalAccessor temporal) {
        return (ChronoZonedDateTime<GJDate>) super.zonedDateTime(temporal);
    }

    /**
     * Obtains a GregorianJulian zoned date-time in this chronology from an {@code Instant}.
     *
     * @param instant  the instant to create the date-time from, not null
     * @param zone  the time-zone, not null
     * @return the GregorianJulian zoned date-time, not null
     * @throws DateTimeException if the result exceeds the supported range
     */
    @Override
    @SuppressWarnings("unchecked")
    public ChronoZonedDateTime<GJDate> zonedDateTime(Instant instant, ZoneId zone) {
        return (ChronoZonedDateTime<GJDate>) super.zonedDateTime(instant, zone);
    }

    //-----------------------------------------------------------------------
    /**
     * Checks if the specified year is a leap year.
     * <p>
     * The result will return the same as {@link JulianChronology#isLeapYear(long)} for
     * year 1582 and earlier, and {@link IsoChronology#isLeapYear(long)} otherwise.
     * This method does not validate the year passed in, and only has a
     * well-defined result for years in the supported range.
     *
     * @param prolepticYear  the proleptic-year to check, not validated for range
     * @return true if the year is a leap year
     */
    @Override
    public boolean isLeapYear(long prolepticYear) {
        if (prolepticYear <= CUTOVER_YEAR) {
            return JulianChronology.INSTANCE.isLeapYear(prolepticYear);
        }
        return IsoChronology.INSTANCE.isLeapYear(prolepticYear);
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
            case DAY_OF_YEAR:
                return DOY_RANGE;
            case ALIGNED_WEEK_OF_MONTH:
                return ALIGNED_WOM_RANGE;
            case ALIGNED_WEEK_OF_YEAR:
                return ALIGNED_WOY_RANGE;
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
    public GJDate resolveDate(Map<TemporalField, Long> fieldValues, ResolverStyle resolverStyle) {
        return (GJDate) super.resolveDate(fieldValues, resolverStyle);
    }

}
