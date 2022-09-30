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

import static org.apache.phoenix.util.chrono.GJChronology.CUTOVER;
import static org.apache.phoenix.util.chrono.GJChronology.CUTOVER_DAYS;
import static org.apache.phoenix.util.chrono.GJChronology.CUTOVER_YEAR;

import java.io.Serializable;
import java.time.Clock;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.chrono.ChronoLocalDate;
import java.time.chrono.ChronoLocalDateTime;
import java.time.chrono.ChronoPeriod;
import java.time.temporal.ChronoField;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAdjuster;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalQueries;
import java.time.temporal.TemporalQuery;
import java.time.temporal.TemporalUnit;
import java.time.temporal.ValueRange;
import java.util.Objects;

/**
 * A date in the GregorianJulian calendar system.
 * <p>
 * This date operates using the {@linkplain GJChronology GregorianJulian calendar}.
 *
 * <h3>Implementation Requirements</h3>
 * This class is immutable and thread-safe.
 * <p>
 * This class must be treated as a value type. Do not synchronize, rely on the
 * identity hash code or use the distinction between equals() and ==.
 */
public final class GJDate
        extends AbstractDateCopy
        implements ChronoLocalDate, Serializable {
    /**
     * Serialization version.
     */
    private static final long serialVersionUID = -9626278512674L;
    /**
     * The underlying date.
     */
    private final LocalDate isoDate;
    /**
     * The underlying Julian date if before the cutover.
     */
    private static transient org.apache.phoenix.util.chrono.JulianDateCopy JulianDateCopy;

    //-----------------------------------------------------------------------
    /**
     * Obtains the current {@code GJDate} from the system clock in the default time-zone.
     * <p>
     * This will query the {@link Clock#systemDefaultZone() system clock} in the default
     * time-zone to obtain the current date.
     * <p>
     * Using this method will prevent the ability to use an alternate clock for testing
     * because the clock is hard-coded.
     *
     * @return the current date using the system clock and default time-zone, not null
     */
    public static GJDate now() {
        return now(Clock.systemDefaultZone());
    }

    /**
     * Obtains the current {@code GJDate} from the system clock in the specified time-zone.
     * <p>
     * This will query the {@link Clock#system(ZoneId) system clock} to obtain the current date.
     * Specifying the time-zone avoids dependence on the default time-zone.
     * <p>
     * Using this method will prevent the ability to use an alternate clock for testing
     * because the clock is hard-coded.
     *
     * @param zone  the zone ID to use, not null
     * @return the current date using the system clock, not null
     */
    public static GJDate now(ZoneId zone) {
        return now(Clock.system(zone));
    }

    /**
     * Obtains the current {@code GJDate} from the specified clock.
     * <p>
     * This will query the specified clock to obtain the current date - today.
     * Using this method allows the use of an alternate clock for testing.
     * The alternate clock may be introduced using {@linkplain Clock dependency injection}.
     *
     * @param clock  the clock to use, not null
     * @return the current date, not null
     * @throws DateTimeException if the current date cannot be obtained
     */
    public static GJDate now(Clock clock) {
        return new GJDate(LocalDate.now(clock));
    }

    /**
     * Obtains a {@code GJDate} representing a date in the GregorianJulian calendar
     * system from the proleptic-year, month-of-year and day-of-month fields.
     * <p>
     * This returns a {@code GJDate} with the specified fields.
     * <p>
     * Dates in the middle of the cutover gap, such as the 10th October 1582,
     * will not throw an exception. Instead, the date will be treated as a Julian date
     * and converted to an ISO date, with the day of month shifted by 10 days.
     * <p>
     * Invalid dates, such as September 31st will throw an exception.
     *
     * @param prolepticYear  the GregorianJulian proleptic-year
     * @param month  the GregorianJulian month-of-year, from 1 to 12
     * @param dayOfMonth  the GregorianJulian day-of-month, from 1 to 31
     * @return the date in GregorianJulian calendar system, not null
     * @throws DateTimeException if the value of any field is out of range,
     *  or if the day-of-month is invalid for the month-year
     */
    public static GJDate of(int prolepticYear, int month, int dayOfMonth) {
        return GJDate.create(prolepticYear, month, dayOfMonth);
    }

    /**
     * Obtains a {@code GJDate} from a temporal object.
     * <p>
     * This obtains a date in the GregorianJulian calendar system based on the specified temporal.
     * A {@code TemporalAccessor} represents an arbitrary set of date and time information,
     * which this factory converts to an instance of {@code GJDate}.
     * <p>
     * The conversion uses the {@link ChronoField#EPOCH_DAY EPOCH_DAY}
     * field, which is standardized across calendar systems.
     * <p>
     * This method matches the signature of the functional interface {@link TemporalQuery}
     * allowing it to be used as a query via method reference, {@code GJDate::from}.
     *
     * @param temporal  the temporal object to convert, not null
     * @return the date in GregorianJulian calendar system, not null
     * @throws DateTimeException if unable to convert to a {@code GJDate}
     */
    public static GJDate from(TemporalAccessor temporal) {
        if (temporal instanceof GJDate) {
            return (GJDate) temporal;
        }
        return new GJDate(LocalDate.from(temporal));
    }

    //-----------------------------------------------------------------------
    /**
     * Obtains a {@code GJDate} representing a date in the GregorianJulian calendar
     * system from the proleptic-year and day-of-year fields.
     * <p>
     * This returns a {@code GJDate} with the specified fields.
     * The day must be valid for the year, otherwise an exception will be thrown.
     *
     * @param prolepticYear  the GregorianJulian proleptic-year
     * @param dayOfYear  the GregorianJulian day-of-year, from 1 to 366
     * @return the date in GregorianJulian calendar system, not null
     * @throws DateTimeException if the value of any field is out of range,
     *  or if the day-of-year is invalid for the year
     */
    static GJDate ofYearDay(int prolepticYear, int dayOfYear) {
        if (prolepticYear < CUTOVER_YEAR || (prolepticYear == CUTOVER_YEAR && dayOfYear <= 246)) {
            JulianDateCopy julian = JulianDateCopy.ofYearDay(prolepticYear, dayOfYear);
            return new GJDate(julian);
        } else if (prolepticYear == CUTOVER_YEAR) {
            LocalDate iso = LocalDate.ofYearDay(prolepticYear, dayOfYear + CUTOVER_DAYS);
            return new GJDate(iso);
        } else {
            LocalDate iso = LocalDate.ofYearDay(prolepticYear, dayOfYear);
            return new GJDate(iso);
        }
    }

    /**
     * Obtains a {@code GJDate} representing a date in the GregorianJulian calendar
     * system from the epoch-day.
     *
     * @param epochDay  the epoch day to convert based on 1970-01-01 (ISO)
     * @return the date in GregorianJulian calendar system, not null
     * @throws DateTimeException if the epoch-day is out of range
     */
    static GJDate ofEpochDay(final long epochDay) {
        return new GJDate(LocalDate.ofEpochDay(epochDay));
    }

    /**
     * Creates a {@code GJDate} validating the input.
     *
     * @param prolepticYear  the GregorianJulian proleptic-year
     * @param month  the GregorianJulian month-of-year, from 1 to 12
     * @param dayOfMonth  the GregorianJulian day-of-month, from 1 to 31
     * @return the date in GregorianJulian calendar system, not null
     * @throws DateTimeException if the value of any field is out of range,
     *  or if the day-of-month is invalid for the month-year
     */
    static GJDate create(int prolepticYear, int month, int dayOfMonth) {
        if (prolepticYear < CUTOVER_YEAR) {
            JulianDateCopy julian = JulianDateCopy.of(prolepticYear, month, dayOfMonth);
            return new GJDate(julian);
        } else {
            LocalDate iso = LocalDate.of(prolepticYear, month, dayOfMonth);
            if (iso.isBefore(CUTOVER)) {
                JulianDateCopy julian = JulianDateCopy.of(prolepticYear, month, dayOfMonth);
                return new GJDate(julian);
            }
            return new GJDate(iso);
        }
    }

    //-----------------------------------------------------------------------
    /**
     * Creates an instance from an ISO date.
     *
     * @param isoDate  the standard local date, not null
     */
    GJDate(LocalDate isoDate) {
        Objects.requireNonNull(isoDate, "isoDate");
        this.isoDate = isoDate;
        JulianDateCopy = (isoDate.isBefore(CUTOVER) ? JulianDateCopy.from(isoDate) : null);
    }

    /**
     * Creates an instance from a Julian date.
     *
     * @param JulianDateCopy  the Julian date before the cutover, not null
     */
    GJDate(JulianDateCopy JulianDateCopy) {
        Objects.requireNonNull(JulianDateCopy, "JulianDateCopy");
        this.isoDate = LocalDate.from(JulianDateCopy);
        GJDate.JulianDateCopy = (isoDate.isBefore(CUTOVER) ? JulianDateCopy : null);
    }

    /**
     * Validates the object.
     *
     * @return the resolved date, not null
     */
    private Object readResolve() {
        return new GJDate(isoDate);
    }

    //-----------------------------------------------------------------------
    private boolean isCutoverYear() {
        return isoDate.getYear() == CUTOVER_YEAR && isoDate.getDayOfYear() > CUTOVER_DAYS;
    }

    private boolean isCutoverMonth() {
        return isoDate.getYear() == CUTOVER_YEAR && isoDate.getMonthValue() == 9 && isoDate.getDayOfMonth() > CUTOVER_DAYS;
    }

    //-------------------------------------------------------------------------
    @Override
    int getAlignedDayOfWeekInMonth() {
        if (isCutoverMonth() && JulianDateCopy == null) {
            return ((getDayOfMonth() - 1 - CUTOVER_DAYS) % lengthOfWeek()) + 1;
        }
        return super.getAlignedDayOfWeekInMonth();
    }

    @Override
    int getAlignedWeekOfMonth() {
        if (isCutoverMonth() && JulianDateCopy == null) {
            return ((getDayOfMonth() - 1 - CUTOVER_DAYS) / lengthOfWeek()) + 1;
        }
        return super.getAlignedWeekOfMonth();
    }

    @Override
    int getProlepticYear() {
        return (JulianDateCopy != null ? JulianDateCopy.getProlepticYear() : isoDate.getYear());
    }

    @Override
    int getMonth() {
        return (JulianDateCopy != null ? JulianDateCopy.getMonth() : isoDate.getMonthValue());
    }

    @Override
    int getDayOfMonth() {
        return (JulianDateCopy != null ? JulianDateCopy.getDayOfMonth() : isoDate.getDayOfMonth());
    }

    @Override
    int getDayOfYear() {
        if (JulianDateCopy != null) {
            return JulianDateCopy.getDayOfYear();
        }
        if (isoDate.getYear() == CUTOVER_YEAR) {
            return isoDate.getDayOfYear() - CUTOVER_DAYS;
        }
        return isoDate.getDayOfYear();
    }

    @Override
    public ValueRange rangeChrono(ChronoField field) {
        switch (field) {
            case DAY_OF_MONTH:
                // short length, but value range still 1 to 30
                if (isCutoverMonth()) {
                    return ValueRange.of(1, 30);
                }
                return ValueRange.of(1, lengthOfMonth());
            case DAY_OF_YEAR:
                // 1 to 356 in cutover year, otherwise 1 to 365/366
                return ValueRange.of(1, lengthOfYear());
            case ALIGNED_WEEK_OF_MONTH:
                // 1 to 3 in cutover month, otherwise 1 to 4/5
                return rangeAlignedWeekOfMonth();
            case ALIGNED_WEEK_OF_YEAR:
                // 1 to 51 in cutover year, otherwise 1 to 53
                if (isCutoverYear()) {
                    return ValueRange.of(1, 51);
                }
                return ChronoField.ALIGNED_WEEK_OF_YEAR.range();
            default:
                return getChronology().range(field);
        }
    }

    @Override
    ValueRange rangeAlignedWeekOfMonth() {
        if (isCutoverMonth()) {
            return ValueRange.of(1, 3);
        }
        return ValueRange.of(1, getMonth() == 2 && isLeapYear() == false ? 4 : 5);
    }

    @Override
    GJDate resolvePrevious(int year, int month, int dayOfMonth) {
        switch (month) {
            case 2:
                dayOfMonth = Math.min(dayOfMonth, getChronology().isLeapYear(year) ? 29 : 28);
                break;
            case 4:
            case 6:
            case 9:
            case 11:
                dayOfMonth = Math.min(dayOfMonth, 30);
                break;
            default:
                break;
        }
        return create(year, month, dayOfMonth);
    }

    //-----------------------------------------------------------------------
    /**
     * Gets the chronology of this date, which is the GregorianJulian calendar system.
     * <p>
     * The {@code Chronology} represents the calendar system in use.
     * The era and other fields in {@link ChronoField} are defined by the chronology.
     *
     * @return the GregorianJulian chronology, not null
     */
    @Override
    public GJChronology getChronology() {
        return GJChronology.INSTANCE;
    }

    /**
     * Gets the era applicable at this date.
     * <p>
     * The GregorianJulian calendar system has two eras, 'AD' and 'BC',
     * defined by {@link JulianEra}.
     *
     * @return the era applicable at this date, not null
     */
    @Override
    public JulianEra getEra() {
        return (getProlepticYear() >= 1 ? JulianEra.AD : JulianEra.BC);
    }

    /**
     * Returns the length of the month represented by this date.
     * <p>
     * This returns the length of the month in days.
     * This takes into account the cutover, returning 21 in October 1582.
     *
     * @return the length of the month in days, from 21 to 31
     */
    @Override
    public int lengthOfMonth() {
        if (isCutoverMonth()) {
            return 21;
        }
        return (JulianDateCopy != null ? JulianDateCopy.lengthOfMonth() : isoDate.lengthOfMonth());
    }

    /**
     * Returns the length of the year represented by this date.
     * <p>
     * This returns the length of the year in days.
     * This takes into account the cutover, returning 356 in 1582.
     *
     * @return the length of the year in days, from 356 to 366
     */
    @Override
    public int lengthOfYear() {
        if (isCutoverYear()) {
            return 356;
        }
        return (JulianDateCopy != null ? JulianDateCopy.lengthOfYear() : isoDate.lengthOfYear());
    }

    //-------------------------------------------------------------------------
    @Override
    public GJDate with(TemporalAdjuster adjuster) {
        return (GJDate) adjuster.adjustInto(this);
    }

    @Override
    public GJDate with(TemporalField field, long newValue) {
        return (GJDate) super.with(field, newValue);
    }

    //-----------------------------------------------------------------------
    @Override
    public GJDate plus(TemporalAmount amount) {
        return (GJDate) amount.addTo(this);
    }

    @Override
    public GJDate plus(long amountToAdd, TemporalUnit unit) {
        return (GJDate) super.plus(amountToAdd, unit);
    }

    @Override
    public GJDate minus(TemporalAmount amount) {
        return (GJDate) amount.subtractFrom(this);
    }

    @Override
    public GJDate minus(long amountToSubtract, TemporalUnit unit) {
        return (amountToSubtract == Long.MIN_VALUE ? plus(Long.MAX_VALUE, unit).plus(1, unit) : plus(-amountToSubtract, unit));
    }

    //-------------------------------------------------------------------------
    @Override  // for covariant return type
    @SuppressWarnings("unchecked")
    public ChronoLocalDateTime<GJDate> atTime(LocalTime localTime) {
        return (ChronoLocalDateTime<GJDate>) super.atTime(localTime);
    }

    @Override
    public long until(Temporal endExclusive, TemporalUnit unit) {
        return super.until(GJDate.from(endExclusive), unit);
    }

    @Override
    public ChronoPeriod until(ChronoLocalDate endDateExclusive) {
        GJDate end = GJDate.from(endDateExclusive);
        long totalMonths = end.getProlepticMonth() - this.getProlepticMonth();  // safe
        int days = end.getDayOfMonth() - this.getDayOfMonth();
        if (totalMonths == 0 && isCutoverMonth()) {
            if (JulianDateCopy != null && end.JulianDateCopy == null) {
                days -= CUTOVER_DAYS;
            } else if (JulianDateCopy == null && end.JulianDateCopy != null) {
                days += CUTOVER_DAYS;
            }
        } else if (totalMonths > 0) {
            if (JulianDateCopy != null && end.JulianDateCopy == null) {
                AbstractDateCopy calcDate = this.plusMonths(totalMonths);
                days = (int) (end.toEpochDay() - calcDate.toEpochDay());  // safe
            }
            if (days < 0) {
                totalMonths--;
                AbstractDateCopy calcDate = this.plusMonths(totalMonths);
                days = (int) (end.toEpochDay() - calcDate.toEpochDay());  // safe
            }
        } else if (totalMonths < 0 && days > 0) {
            totalMonths++;
            AbstractDateCopy calcDate = this.plusMonths(totalMonths);
            days = (int) (end.toEpochDay() - calcDate.toEpochDay());  // safe
        }
        int years = Math.toIntExact(totalMonths / lengthOfYearInMonths());  // safe
        int months = (int) (totalMonths % lengthOfYearInMonths());  // safe
        return getChronology().period(years, months, days);
    }

    //-----------------------------------------------------------------------
    @Override
    public long toEpochDay() {
        return isoDate.toEpochDay();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <R> R query(TemporalQuery<R> query) {
        if (query == TemporalQueries.localDate()) {
            return (R) isoDate;
        }
        return super.query(query);
    }

    //-------------------------------------------------------------------------
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof GJDate) {
            GJDate otherDate = (GJDate) obj;
            return this.isoDate.equals(otherDate.isoDate);
        }
        return false;
    }

    /**
     * A hash code for this date.
     *
     * @return a suitable hash code based only on the Chronology and the date
     */
    @Override
    public int hashCode() {
        return getChronology().getId().hashCode() ^ isoDate.hashCode();
    }

}
