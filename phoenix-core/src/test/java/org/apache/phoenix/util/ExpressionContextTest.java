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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.Test;

/**
 * Test class for {@link CompliantExpressionContext}
 *
 */
public class ExpressionContextTest {

    private static long HALF_SEC = 500;
    private static long SEC = 2 * HALF_SEC;

    private static long HALF_MIN = 30 * 1000;
    private static long MIN = 2 * HALF_MIN;

    private static long HALF_HOUR = 30 * 60 * 1000;
    private static long HOUR = 2 * HALF_HOUR;

    private static long HALF_DAY = 12 * 60 * 60 * 1000;
    private static long DAY = 2 * HALF_DAY;
    
    private static long HALF_WEEK = 7 * 12 * 60 * 60 * 1000;
    private static long WEEK = 2 * HALF_WEEK;
    
    // Note that without the "l" the integer arithmetics below would overflow
    private static long HALF_YEAR = 365l * 12 * 60 * 60 * 1000;
    private static long YEAR = 2l * HALF_YEAR;

    // Not really EST, it resolves to America/New_York
    private static final ExpressionContext EST_CONTEXT =
            new CompliantExpressionContext(null, null, null, "EST");

    private static final ExpressionContext GMT_CONTEXT =
            ExpressionContextFactory.getGMTServerSide();

    @Test
    public void testRoundingCompliant() {
        // The odd/even distinction doesn't apply since switching to roundHalfCeiling

        java.sql.Timestamp oddWholeSecond =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 11:11:11").getTime());
        long lowerBoundaryOddWholeSecond = oddWholeSecond.getTime() - HALF_SEC;
        long upperBoundaryOddWholeSecond = oddWholeSecond.getTime() + HALF_SEC -1;
        assertEquals(lowerBoundaryOddWholeSecond, EST_CONTEXT.roundSecondLower(oddWholeSecond.getTime(), 0, 1));
        assertEquals(upperBoundaryOddWholeSecond, EST_CONTEXT.roundSecondUpper(oddWholeSecond.getTime(), 0, 1));
        assertEquals(oddWholeSecond,
            new java.sql.Timestamp(EST_CONTEXT.roundSecond(lowerBoundaryOddWholeSecond, 0, 1)));
        assertNotEquals(oddWholeSecond,
            new java.sql.Timestamp(EST_CONTEXT.roundSecond(lowerBoundaryOddWholeSecond -1, 0, 1)));
        assertEquals(oddWholeSecond,
            new java.sql.Timestamp(EST_CONTEXT.roundSecond(upperBoundaryOddWholeSecond, 0, 1)));
        assertNotEquals(oddWholeSecond,
            new java.sql.Timestamp(EST_CONTEXT.roundSecond(upperBoundaryOddWholeSecond + 1, 0, 1)));
        
        // 10 sec range
        java.sql.Timestamp oddWholeSecondRound10 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 11:11:10").getTime());
        long lowerBoundaryOddWholeSecondRound10 = oddWholeSecondRound10.getTime() - 5 * SEC;
        long upperBoundaryOddWholeSecondRound10 = oddWholeSecondRound10.getTime() + 5 * SEC -1;
        assertEquals(lowerBoundaryOddWholeSecondRound10, EST_CONTEXT.roundSecondLower(oddWholeSecond.getTime(), 0, 10));
        assertEquals(upperBoundaryOddWholeSecondRound10, EST_CONTEXT.roundSecondUpper(oddWholeSecond.getTime(), 0, 10));
        assertEquals(oddWholeSecondRound10,
            new java.sql.Timestamp(EST_CONTEXT.roundSecond(lowerBoundaryOddWholeSecondRound10, 0, 10)));
        assertNotEquals(oddWholeSecondRound10,
            new java.sql.Timestamp(EST_CONTEXT.roundSecond(lowerBoundaryOddWholeSecondRound10 -1, 0, 10)));
        assertEquals(oddWholeSecondRound10,
            new java.sql.Timestamp(EST_CONTEXT.roundSecond(upperBoundaryOddWholeSecondRound10, 0, 10)));
        assertNotEquals(oddWholeSecondRound10,
            new java.sql.Timestamp(EST_CONTEXT.roundSecond(upperBoundaryOddWholeSecondRound10 + 1, 0, 10)));
        
        // 15 sec range
        java.sql.Timestamp oddWholeSecondRound15 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 11:11:15").getTime());
        long lowerBoundaryOddWholeSecondRound15 = oddWholeSecondRound15.getTime() - 15 * HALF_SEC;
        long upperBoundaryOddWholeSecondRound15 = oddWholeSecondRound15.getTime() + 15 * HALF_SEC -1;
        assertEquals(lowerBoundaryOddWholeSecondRound15, EST_CONTEXT.roundSecondLower(oddWholeSecond.getTime(), 0, 15));
        assertEquals(upperBoundaryOddWholeSecondRound15, EST_CONTEXT.roundSecondUpper(oddWholeSecond.getTime(), 0, 15));
        assertEquals(oddWholeSecondRound15,
            new java.sql.Timestamp(EST_CONTEXT.roundSecond(lowerBoundaryOddWholeSecondRound15, 0, 15)));
        assertNotEquals(oddWholeSecondRound15,
            new java.sql.Timestamp(EST_CONTEXT.roundSecond(lowerBoundaryOddWholeSecondRound15 -1, 0, 15)));
        assertEquals(oddWholeSecondRound15,
            new java.sql.Timestamp(EST_CONTEXT.roundSecond(upperBoundaryOddWholeSecondRound15, 0, 15)));
        assertNotEquals(oddWholeSecondRound15,
            new java.sql.Timestamp(EST_CONTEXT.roundSecond(upperBoundaryOddWholeSecondRound15 + 1, 0, 15)));
        
        java.sql.Timestamp evenWholeSecond =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 11:11:12").getTime());
        long lowerBoundaryEvenWholeSecond = evenWholeSecond.getTime() - HALF_SEC;
        long upperBoundaryEvenWholeSecond = evenWholeSecond.getTime() + HALF_SEC -1;
        assertEquals(lowerBoundaryEvenWholeSecond, EST_CONTEXT.roundSecondLower(evenWholeSecond.getTime(), 0, 1));
        assertEquals(upperBoundaryEvenWholeSecond, EST_CONTEXT.roundSecondUpper(evenWholeSecond.getTime(), 0, 1));
        assertEquals(evenWholeSecond,
            new java.sql.Timestamp(EST_CONTEXT.roundSecond(lowerBoundaryEvenWholeSecond, 0, 1)));
        assertNotEquals(evenWholeSecond,
            new java.sql.Timestamp(EST_CONTEXT.roundSecond(lowerBoundaryEvenWholeSecond - 1, 0, 1)));
        assertEquals(evenWholeSecond,
            new java.sql.Timestamp(EST_CONTEXT.roundSecond(upperBoundaryEvenWholeSecond, 0, 1)));
        assertNotEquals(evenWholeSecond,
            new java.sql.Timestamp(EST_CONTEXT.roundSecond(upperBoundaryEvenWholeSecond + 1, 0, 1)));

        java.sql.Timestamp oddWholeMinute =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 11:11:0").getTime());
        long lowerBoundaryOddWholeMinute = oddWholeMinute.getTime() - HALF_MIN;
        long upperBoundaryOddWholeMinute = oddWholeMinute.getTime() + HALF_MIN - 1;
        assertEquals(lowerBoundaryOddWholeMinute, EST_CONTEXT.roundMinuteLower(oddWholeMinute.getTime(), 0, 1));
        assertEquals(upperBoundaryOddWholeMinute, EST_CONTEXT.roundMinuteUpper(oddWholeMinute.getTime(), 0, 1));
        assertEquals(oddWholeMinute,
            new java.sql.Timestamp(EST_CONTEXT.roundMinute(lowerBoundaryOddWholeMinute, 0, 1)));
        assertNotEquals(oddWholeMinute,
            new java.sql.Timestamp(EST_CONTEXT.roundMinute(lowerBoundaryOddWholeMinute -1, 0, 1)));
        assertEquals(oddWholeMinute,
            new java.sql.Timestamp(EST_CONTEXT.roundMinute(upperBoundaryOddWholeMinute, 0, 1)));
        assertNotEquals(oddWholeMinute,
            new java.sql.Timestamp(EST_CONTEXT.roundMinute(upperBoundaryOddWholeMinute + 1, 0, 1)));

        java.sql.Timestamp oddWholeMinuteRound20 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 11:20:0").getTime());
        long lowerBoundaryOddWholeMinute20 = oddWholeMinuteRound20.getTime() - 10 * MIN;
        long upperBoundaryOddWholeMinute20 = oddWholeMinuteRound20.getTime() + 10 * MIN - 1;
        assertEquals(lowerBoundaryOddWholeMinute20, EST_CONTEXT.roundMinuteLower(oddWholeMinute.getTime(), 0, 20));
        assertEquals(upperBoundaryOddWholeMinute20, EST_CONTEXT.roundMinuteUpper(oddWholeMinute.getTime(), 0, 20));
        assertEquals(oddWholeMinuteRound20,
            new java.sql.Timestamp(EST_CONTEXT.roundMinute(lowerBoundaryOddWholeMinute20, 0, 20)));
        assertNotEquals(oddWholeMinuteRound20,
            new java.sql.Timestamp(EST_CONTEXT.roundMinute(lowerBoundaryOddWholeMinute20 -1, 0, 20)));
        assertEquals(oddWholeMinuteRound20,
            new java.sql.Timestamp(EST_CONTEXT.roundMinute(upperBoundaryOddWholeMinute20, 0, 20)));
        assertNotEquals(oddWholeMinuteRound20,
            new java.sql.Timestamp(EST_CONTEXT.roundMinute(upperBoundaryOddWholeMinute20 + 1, 0, 20)));
        
        //Minutes since epoch, don't expect the rounded value to be "round"
        java.sql.Timestamp oddWholeMinuteRound17 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 11:12:00").getTime());
        long lowerBoundaryOddWholeMinute17 = oddWholeMinuteRound17.getTime() - 17 * HALF_MIN;
        long upperBoundaryOddWholeMinute17 = oddWholeMinuteRound17.getTime() + 17 * HALF_MIN - 1;
        assertEquals(lowerBoundaryOddWholeMinute17, EST_CONTEXT.roundMinuteLower(oddWholeMinute.getTime(), 0, 17));
        assertEquals(upperBoundaryOddWholeMinute17, EST_CONTEXT.roundMinuteUpper(oddWholeMinute.getTime(), 0, 17));
        assertEquals(oddWholeMinuteRound17,
            new java.sql.Timestamp(EST_CONTEXT.roundMinute(lowerBoundaryOddWholeMinute17, 0, 17)));
        assertNotEquals(oddWholeMinuteRound17,
            new java.sql.Timestamp(EST_CONTEXT.roundMinute(lowerBoundaryOddWholeMinute17 -1, 0, 17)));
        assertEquals(oddWholeMinuteRound17,
            new java.sql.Timestamp(EST_CONTEXT.roundMinute(upperBoundaryOddWholeMinute17, 0, 17)));
        assertNotEquals(oddWholeMinuteRound17,
            new java.sql.Timestamp(EST_CONTEXT.roundMinute(upperBoundaryOddWholeMinute17 + 1, 0, 17)));
        
        java.sql.Timestamp evenWholeMinute =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 11:12:0").getTime());
        long lowerBoundaryEvenWholeMinute = evenWholeMinute.getTime() - HALF_MIN;
        long upperBoundaryEvenWholeMinute = evenWholeMinute.getTime() + HALF_MIN -1;
        assertEquals(lowerBoundaryEvenWholeMinute, EST_CONTEXT.roundMinuteLower(evenWholeMinute.getTime(), 0, 1));
        assertEquals(upperBoundaryEvenWholeMinute, EST_CONTEXT.roundMinuteUpper(evenWholeMinute.getTime(), 0, 1));
        assertEquals(evenWholeMinute,
            new java.sql.Timestamp(EST_CONTEXT.roundMinute(lowerBoundaryEvenWholeMinute, 0, 1)));
        assertNotEquals(evenWholeMinute,
            new java.sql.Timestamp(EST_CONTEXT.roundMinute(lowerBoundaryEvenWholeMinute - 1, 0, 1)));
        assertEquals(evenWholeMinute,
            new java.sql.Timestamp(EST_CONTEXT.roundMinute(upperBoundaryEvenWholeMinute, 0, 1)));
        assertNotEquals(evenWholeMinute,
            new java.sql.Timestamp(EST_CONTEXT.roundMinute(upperBoundaryEvenWholeMinute + 1, 0, 1)));

        java.sql.Timestamp oddWholeHour =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 11:0:0").getTime());
        long lowerBoundaryOddWholeHour = oddWholeHour.getTime() - HALF_HOUR;
        long upperBoundaryOddWholeHour = oddWholeHour.getTime() + HALF_HOUR - 1;
        assertEquals(lowerBoundaryOddWholeHour, EST_CONTEXT.roundHourLower(oddWholeHour.getTime(), 0, 1));
        assertEquals(upperBoundaryOddWholeHour, EST_CONTEXT.roundHourUpper(oddWholeHour.getTime(), 0, 1));
        assertEquals(oddWholeHour,
            new java.sql.Timestamp(EST_CONTEXT.roundHour(lowerBoundaryOddWholeHour, 0, 1)));
        assertNotEquals(oddWholeHour,
            new java.sql.Timestamp(EST_CONTEXT.roundHour(lowerBoundaryOddWholeHour - 1, 0, 1)));
        assertEquals(oddWholeHour,
            new java.sql.Timestamp(EST_CONTEXT.roundHour(upperBoundaryOddWholeHour, 0, 1)));
        assertNotEquals(oddWholeHour,
            new java.sql.Timestamp(EST_CONTEXT.roundHour(upperBoundaryOddWholeHour + 1, 0, 1)));
        
        //Not rounding to hourOfDay
        java.sql.Timestamp oddWholeHour10 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 12:0:0").getTime());
        long lowerBoundaryOddWholeHour10 = oddWholeHour10.getTime() - HALF_HOUR * 10;
        long upperBoundaryOddWholeHour10 = oddWholeHour10.getTime() + HALF_HOUR * 10 - 1;
        assertEquals(lowerBoundaryOddWholeHour10, EST_CONTEXT.roundHourLower(oddWholeHour.getTime(), 0, 10));
        assertEquals(upperBoundaryOddWholeHour10, EST_CONTEXT.roundHourUpper(oddWholeHour.getTime(), 0, 10));
        assertEquals(oddWholeHour10,
            new java.sql.Timestamp(EST_CONTEXT.roundHour(lowerBoundaryOddWholeHour10, 0, 10)));
        assertNotEquals(oddWholeHour10,
            new java.sql.Timestamp(EST_CONTEXT.roundHour(lowerBoundaryOddWholeHour10 - 1, 0, 10)));
        assertEquals(oddWholeHour10,
            new java.sql.Timestamp(EST_CONTEXT.roundHour(upperBoundaryOddWholeHour10, 0, 10)));
        assertNotEquals(oddWholeHour10,
            new java.sql.Timestamp(EST_CONTEXT.roundHour(upperBoundaryOddWholeHour10 + 1, 0, 10)));
        
        //Not rounding to hourOfDay
        java.sql.Timestamp oddWholeHour11 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 07:0:0").getTime());
        long lowerBoundaryOddWholeHour11 = oddWholeHour11.getTime() - HALF_HOUR * 11;
        long upperBoundaryOddWholeHour11 = oddWholeHour11.getTime() + HALF_HOUR * 11 - 1;
        assertEquals(lowerBoundaryOddWholeHour11, EST_CONTEXT.roundHourLower(oddWholeHour.getTime(), 0, 11));
        assertEquals(upperBoundaryOddWholeHour11, EST_CONTEXT.roundHourUpper(oddWholeHour.getTime(), 0, 11));
        assertEquals(oddWholeHour11,
            new java.sql.Timestamp(EST_CONTEXT.roundHour(lowerBoundaryOddWholeHour11, 0, 11)));
        assertNotEquals(oddWholeHour11,
            new java.sql.Timestamp(EST_CONTEXT.roundHour(lowerBoundaryOddWholeHour11 - 1, 0, 11)));
        assertEquals(oddWholeHour11,
            new java.sql.Timestamp(EST_CONTEXT.roundHour(upperBoundaryOddWholeHour11, 0, 11)));
        assertNotEquals(oddWholeHour11,
            new java.sql.Timestamp(EST_CONTEXT.roundHour(upperBoundaryOddWholeHour11 + 1, 0, 11)));
        
        java.sql.Timestamp evenwholeHour =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 12:0:0").getTime());
        long lowerBoundaryEvenWholeHour = evenwholeHour.getTime() - HALF_HOUR;
        long upperBoundaryEvenWholeHour = evenwholeHour.getTime() + HALF_HOUR - 1;
        assertEquals(lowerBoundaryEvenWholeHour, EST_CONTEXT.roundHourLower(evenwholeHour.getTime(), 0, 1));
        assertEquals(upperBoundaryEvenWholeHour, EST_CONTEXT.roundHourUpper(evenwholeHour.getTime(), 0, 1));
        assertEquals(evenwholeHour,
            new java.sql.Timestamp(EST_CONTEXT.roundHour(lowerBoundaryEvenWholeHour, 0, 1)));
        assertNotEquals(evenwholeHour,
            new java.sql.Timestamp(EST_CONTEXT.roundHour(lowerBoundaryEvenWholeHour -1, 0, 1)));
        assertEquals(evenwholeHour,
            new java.sql.Timestamp(EST_CONTEXT.roundHour(upperBoundaryEvenWholeHour, 0, 1)));
        assertNotEquals(evenwholeHour,
            new java.sql.Timestamp(EST_CONTEXT.roundHour(upperBoundaryEvenWholeHour + 1, 0, 1)));
        
        // From Days and larger periods, we're using proper date arithmetics 
        // No DST switchover
        java.sql.Timestamp oddWholeDay =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 0:0:0").getTime());
        long lowerBoundaryOddWholeDay = oddWholeDay.getTime() - HALF_DAY;
        long upperBoundaryOddWholeDay = oddWholeDay.getTime() + HALF_DAY - 1;
        assertEquals(lowerBoundaryOddWholeDay, EST_CONTEXT.roundDayLower(oddWholeDay.getTime(), 0, 1));
        assertEquals(upperBoundaryOddWholeDay, EST_CONTEXT.roundDayUpper(oddWholeDay.getTime(), 0, 1));
        assertEquals(oddWholeDay,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(lowerBoundaryOddWholeDay, 0, 1)));
        assertNotEquals(oddWholeDay,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(lowerBoundaryOddWholeDay - 1, 0, 1)));
        assertEquals(oddWholeDay,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(upperBoundaryOddWholeDay, 0, 1)));
        assertNotEquals(oddWholeDay,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(upperBoundaryOddWholeDay + 1, 0, 1)));
        
        java.sql.Timestamp oddWholeDay10 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-14 0:0:0").getTime());
        long lowerBoundaryOddWholeDay10 = oddWholeDay10.getTime() - 10 * HALF_DAY;
        long upperBoundaryOddWholeDay10 = oddWholeDay10.getTime() + 10 * HALF_DAY - 1;
        assertEquals(lowerBoundaryOddWholeDay10, EST_CONTEXT.roundDayLower(oddWholeDay.getTime(), 0, 10));
        assertEquals(upperBoundaryOddWholeDay10, EST_CONTEXT.roundDayUpper(oddWholeDay.getTime(), 0, 10));
        assertEquals(oddWholeDay10,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(lowerBoundaryOddWholeDay10, 0, 10)));
        assertNotEquals(oddWholeDay10,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(lowerBoundaryOddWholeDay10 - 1, 0, 10)));
        assertEquals(oddWholeDay10,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(upperBoundaryOddWholeDay10, 0, 10)));
        assertNotEquals(oddWholeDay10,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(upperBoundaryOddWholeDay10 + 1, 0, 10)));

        java.sql.Timestamp oddWholeDay3 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-12 0:0:0").getTime());
        long lowerBoundaryOddWholeDay3 = oddWholeDay3.getTime() - 3 * HALF_DAY;
        long upperBoundaryOddWholeDay3 = oddWholeDay3.getTime() + 3 * HALF_DAY - 1;
        assertEquals(lowerBoundaryOddWholeDay3, EST_CONTEXT.roundDayLower(oddWholeDay.getTime(), 0, 3));
        assertEquals(upperBoundaryOddWholeDay3, EST_CONTEXT.roundDayUpper(oddWholeDay.getTime(), 0, 3));
        assertEquals(oddWholeDay3,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(lowerBoundaryOddWholeDay3, 0, 3)));
        assertNotEquals(oddWholeDay3,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(lowerBoundaryOddWholeDay3 - 1, 0, 3)));
        assertEquals(oddWholeDay3,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(upperBoundaryOddWholeDay3, 0, 3)));
        assertNotEquals(oddWholeDay3,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(upperBoundaryOddWholeDay3 + 1, 0, 3)));
        
        java.sql.Timestamp evenWholeDay =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-12 0:0:0").getTime());
        long lowerBoundaryEvenWholeDay = evenWholeDay.getTime() - HALF_DAY;
        long upperBoundaryEvenWholeDay = evenWholeDay.getTime() + HALF_DAY - 1;
        assertEquals(lowerBoundaryEvenWholeDay, EST_CONTEXT.roundDayLower(evenWholeDay.getTime(), 0, 1));
        assertEquals(upperBoundaryEvenWholeDay, EST_CONTEXT.roundDayUpper(evenWholeDay.getTime(), 0, 1));
        assertEquals(evenWholeDay,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(lowerBoundaryEvenWholeDay, 0, 1)));
        assertNotEquals(evenWholeDay,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(lowerBoundaryEvenWholeDay -1, 0, 1)));
        assertEquals(evenWholeDay,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(upperBoundaryEvenWholeDay, 0, 1)));
        assertNotEquals(evenWholeDay,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(upperBoundaryEvenWholeDay + 1, 0, 1)));

        //DST switchover day
        java.sql.Timestamp wholeDayDst23Odd =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-03-13 0:0:0").getTime());
        long lowerBoundaryEvenWholeDayDst23Odd = wholeDayDst23Odd.getTime() - HALF_DAY;
        long upperBoundaryEvenWholeDayDst23Odd = wholeDayDst23Odd.getTime() + (HALF_DAY - HALF_HOUR) - 1;
        assertEquals(lowerBoundaryEvenWholeDayDst23Odd, EST_CONTEXT.roundDayLower(wholeDayDst23Odd.getTime(), 0, 1));
        assertEquals(upperBoundaryEvenWholeDayDst23Odd, EST_CONTEXT.roundDayUpper(wholeDayDst23Odd.getTime(), 0, 1));
        assertEquals(wholeDayDst23Odd,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(lowerBoundaryEvenWholeDayDst23Odd, 0, 1)));
        assertNotEquals(wholeDayDst23Odd,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(lowerBoundaryEvenWholeDayDst23Odd - 1, 0, 1)));
        assertEquals(wholeDayDst23Odd,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(upperBoundaryEvenWholeDayDst23Odd, 0, 1)));
        assertNotEquals(wholeDayDst23Odd,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(upperBoundaryEvenWholeDayDst23Odd + 1, 0, 1)));
        
        //DST switchover previous day
        java.sql.Timestamp wholeDayDst23OddNext =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-03-14 0:0:0").getTime());
        long lowerBoundaryEvenWholeDayDst23OddPrev = wholeDayDst23OddNext.getTime() - (HALF_DAY - HALF_HOUR);
        long upperBoundaryEvenWholeDayDst23OddPrev = wholeDayDst23OddNext.getTime() + HALF_DAY - 1;
        assertEquals(lowerBoundaryEvenWholeDayDst23OddPrev, EST_CONTEXT.roundDayLower(wholeDayDst23OddNext.getTime(), 0, 1));
        assertEquals(upperBoundaryEvenWholeDayDst23OddPrev, EST_CONTEXT.roundDayUpper(wholeDayDst23OddNext.getTime(), 0, 1));
        assertEquals(wholeDayDst23OddNext,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(lowerBoundaryEvenWholeDayDst23OddPrev, 0, 1)));
        assertNotEquals(wholeDayDst23OddNext,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(lowerBoundaryEvenWholeDayDst23OddPrev - 1, 0, 1)));
        assertEquals(wholeDayDst23OddNext,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(upperBoundaryEvenWholeDayDst23OddPrev, 0, 1)));
        assertNotEquals(wholeDayDst23OddNext,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(upperBoundaryEvenWholeDayDst23OddPrev + 1, 0, 1)));
        
        java.sql.Timestamp wholeDayDst23Odd10 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-03-09 0:0:0").getTime());
        long lowerBoundaryEvenWholeDayDst23Odd10 = wholeDayDst23Odd10.getTime() - 10 * HALF_DAY;
        long upperBoundaryEvenWholeDayDst23Odd10 = wholeDayDst23Odd10.getTime() + (10 * HALF_DAY - HOUR) - 1;
        assertEquals(lowerBoundaryEvenWholeDayDst23Odd10, EST_CONTEXT.roundDayLower(wholeDayDst23Odd.getTime(), 0, 10));
        assertEquals(upperBoundaryEvenWholeDayDst23Odd10, EST_CONTEXT.roundDayUpper(wholeDayDst23Odd.getTime(), 0, 10));
        assertEquals(wholeDayDst23Odd10,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(lowerBoundaryEvenWholeDayDst23Odd10, 0, 10)));
        assertNotEquals(wholeDayDst23Odd10,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(lowerBoundaryEvenWholeDayDst23Odd10 - 1, 0, 10)));
        assertEquals(wholeDayDst23Odd10,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(upperBoundaryEvenWholeDayDst23Odd10, 0, 10)));
        assertNotEquals(wholeDayDst23Odd10,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(upperBoundaryEvenWholeDayDst23Odd10 + 1, 0, 10)));
        
        java.sql.Timestamp wholeDayDst23Odd27 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-03-11 0:0:0").getTime());
        long lowerBoundaryEvenWholeDayDst23Odd27 = wholeDayDst23Odd27.getTime() - 27 * HALF_DAY;
        long upperBoundaryEvenWholeDayDst23Odd27 = wholeDayDst23Odd27.getTime() + (27 * HALF_DAY - HOUR) - 1;
        assertEquals(lowerBoundaryEvenWholeDayDst23Odd27, EST_CONTEXT.roundDayLower(wholeDayDst23Odd.getTime(), 0, 27));
        assertEquals(upperBoundaryEvenWholeDayDst23Odd27, EST_CONTEXT.roundDayUpper(wholeDayDst23Odd.getTime(), 0, 27));
        assertEquals(wholeDayDst23Odd27,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(lowerBoundaryEvenWholeDayDst23Odd27, 0, 27)));
        assertNotEquals(wholeDayDst23Odd27,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(lowerBoundaryEvenWholeDayDst23Odd27 - 1, 0, 27)));
        assertEquals(wholeDayDst23Odd27,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(upperBoundaryEvenWholeDayDst23Odd27, 0, 27)));
        assertNotEquals(wholeDayDst23Odd27,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(upperBoundaryEvenWholeDayDst23Odd27 + 1, 0, 27)));
        
        //DST switchover day (13th is 23 hours long)
        java.sql.Timestamp wholeDayDst23Even =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2023-03-12 0:0:0").getTime());
        long lowerBoundaryEvenWholeDayDst23Even = wholeDayDst23Even.getTime() - HALF_DAY;
        long upperBoundaryEvenWholeDayDst23Even = wholeDayDst23Even.getTime() + (HALF_DAY - HALF_HOUR) - 1;
        assertEquals(lowerBoundaryEvenWholeDayDst23Even, EST_CONTEXT.roundDayLower(wholeDayDst23Even.getTime(), 0, 1));
        assertEquals(upperBoundaryEvenWholeDayDst23Even, EST_CONTEXT.roundDayUpper(wholeDayDst23Even.getTime(), 0, 1));
        assertEquals(wholeDayDst23Even,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(lowerBoundaryEvenWholeDayDst23Even, 0, 1)));
        assertNotEquals(wholeDayDst23Even,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(lowerBoundaryEvenWholeDayDst23Even -1, 0, 1)));
        assertEquals(wholeDayDst23Even,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(upperBoundaryEvenWholeDayDst23Even, 0, 1)));
        assertNotEquals(wholeDayDst23Even,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(upperBoundaryEvenWholeDayDst23Even + 1, 0, 1)));

        //DST switchover day
        java.sql.Timestamp wholeDayDst25Odd =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2023-11-05 0:0:0").getTime());
        long lowerBoundaryEvenWholeDayDst25Odd = wholeDayDst25Odd.getTime() - HALF_DAY;
        long upperBoundaryEvenWholeDayDst25Odd = wholeDayDst25Odd.getTime() + HALF_DAY + HALF_HOUR - 1;
        assertEquals(lowerBoundaryEvenWholeDayDst25Odd, EST_CONTEXT.roundDayLower(wholeDayDst25Odd.getTime(), 0, 1));
        assertEquals(upperBoundaryEvenWholeDayDst25Odd, EST_CONTEXT.roundDayUpper(wholeDayDst25Odd.getTime(), 0, 1));
        assertEquals(wholeDayDst25Odd,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(lowerBoundaryEvenWholeDayDst25Odd, 0, 1)));
        assertNotEquals(wholeDayDst25Odd,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(lowerBoundaryEvenWholeDayDst25Odd -1, 0, 1)));
        assertEquals(wholeDayDst25Odd,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(upperBoundaryEvenWholeDayDst25Odd, 0, 1)));
        assertNotEquals(wholeDayDst25Odd,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(upperBoundaryEvenWholeDayDst25Odd + 1, 0, 1)));

        //DST switchover day (6th is day is 25 hours long)
        java.sql.Timestamp wholeDayDst25Even =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-06 0:0:0").getTime());
        long lowerBoundaryEvenWholeDayDst25Even = wholeDayDst25Even.getTime() - HALF_DAY;
        long upperBoundaryEvenWholeDayDst25Even = wholeDayDst25Even.getTime() + HALF_DAY + HALF_HOUR -1;
        assertEquals(lowerBoundaryEvenWholeDayDst25Even, EST_CONTEXT.roundDayLower(wholeDayDst25Even.getTime(), 0, 1));
        assertEquals(upperBoundaryEvenWholeDayDst25Even, EST_CONTEXT.roundDayUpper(wholeDayDst25Even.getTime(), 0, 1));
        assertEquals(wholeDayDst25Even,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(lowerBoundaryEvenWholeDayDst25Even, 0, 1)));
        assertNotEquals(wholeDayDst25Even,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(lowerBoundaryEvenWholeDayDst25Even -1, 0, 1)));
        assertEquals(wholeDayDst25Even,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(upperBoundaryEvenWholeDayDst25Even, 0, 1)));
        assertNotEquals(wholeDayDst25Even,
            new java.sql.Timestamp(EST_CONTEXT.roundDay(upperBoundaryEvenWholeDayDst25Even + 1, 0, 1)));

        java.sql.Timestamp wholeWeekOdd =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-10-10 0:0:0").getTime());
        long lowerBoundaryWholeWeekOdd = wholeWeekOdd.getTime() - HALF_WEEK;
        long upperBoundaryWholeWeekOdd = wholeWeekOdd.getTime() + HALF_WEEK - 1;
        assertEquals(lowerBoundaryWholeWeekOdd, EST_CONTEXT.roundWeekLower(wholeWeekOdd.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeWeekOdd, EST_CONTEXT.roundWeekUpper(wholeWeekOdd.getTime(), 0, 1));
        assertEquals(wholeWeekOdd,
            new java.sql.Timestamp(EST_CONTEXT.roundWeek(lowerBoundaryWholeWeekOdd, 0, 1)));
        assertNotEquals(wholeWeekOdd,
            new java.sql.Timestamp(EST_CONTEXT.roundWeek(lowerBoundaryWholeWeekOdd -1, 0, 1)));
        assertEquals(wholeWeekOdd,
            new java.sql.Timestamp(EST_CONTEXT.roundWeek(upperBoundaryWholeWeekOdd, 0, 1)));
        assertNotEquals(wholeWeekOdd,
            new java.sql.Timestamp(EST_CONTEXT.roundWeek(upperBoundaryWholeWeekOdd + 1, 0, 1)));

        java.sql.Timestamp wholeWeekEven =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-10-17 0:0:0").getTime());
        long lowerBoundaryWholeWeekEven = wholeWeekEven.getTime() - HALF_WEEK;
        long upperBoundaryWholeWeekEven = wholeWeekEven.getTime() + HALF_WEEK -1;
        assertEquals(lowerBoundaryWholeWeekEven, EST_CONTEXT.roundWeekLower(wholeWeekEven.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeWeekEven, EST_CONTEXT.roundWeekUpper(wholeWeekEven.getTime(), 0, 1));
        assertEquals(wholeWeekEven,
            new java.sql.Timestamp(EST_CONTEXT.roundWeek(lowerBoundaryWholeWeekEven, 0, 1)));
        assertNotEquals(wholeWeekEven,
            new java.sql.Timestamp(EST_CONTEXT.roundWeek(lowerBoundaryWholeWeekEven - 1, 0, 1)));
        assertEquals(wholeWeekEven,
            new java.sql.Timestamp(EST_CONTEXT.roundWeek(upperBoundaryWholeWeekEven, 0, 1)));
        assertNotEquals(wholeWeekEven,
            new java.sql.Timestamp(EST_CONTEXT.roundWeek(upperBoundaryWholeWeekEven + 1, 0, 1)));

        java.sql.Timestamp wholeWeekDst25Even =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-10-31 0:0:0").getTime());
        long lowerBoundaryWholeWeekDst25Even = wholeWeekDst25Even.getTime() - HALF_WEEK;
        long upperBoundaryWholeWeekDst25Even = wholeWeekDst25Even.getTime() + HALF_WEEK + HALF_HOUR -1;
        assertEquals(lowerBoundaryWholeWeekDst25Even, EST_CONTEXT.roundWeekLower(wholeWeekDst25Even.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeWeekDst25Even, EST_CONTEXT.roundWeekUpper(wholeWeekDst25Even.getTime(), 0, 1));
        assertEquals(wholeWeekDst25Even,
            new java.sql.Timestamp(EST_CONTEXT.roundWeek(lowerBoundaryWholeWeekDst25Even, 0, 1)));
        assertNotEquals(wholeWeekDst25Even,
            new java.sql.Timestamp(EST_CONTEXT.roundWeek(lowerBoundaryWholeWeekDst25Even - 1, 0, 1)));
        assertEquals(wholeWeekDst25Even,
            new java.sql.Timestamp(EST_CONTEXT.roundWeek(upperBoundaryWholeWeekDst25Even, 0, 1)));
        assertNotEquals(wholeWeekDst25Even,
            new java.sql.Timestamp(EST_CONTEXT.roundWeek(upperBoundaryWholeWeekDst25Even + 1, 0, 1)));

        // Is it even possible for the switchover to be on an Odd week (of Year) ?
        
        java.sql.Timestamp wholeWeekDst23Even =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-03-07 0:0:0").getTime());
        long lowerBoundaryWholeWeekDst23Even = wholeWeekDst23Even.getTime() - HALF_WEEK;
        long upperBoundaryWholeWeekDst23Even = wholeWeekDst23Even.getTime() + (HALF_WEEK - HALF_HOUR) -1;
        assertEquals(lowerBoundaryWholeWeekDst23Even, EST_CONTEXT.roundWeekLower(wholeWeekDst23Even.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeWeekDst23Even, EST_CONTEXT.roundWeekUpper(wholeWeekDst23Even.getTime(), 0, 1));
        assertEquals(wholeWeekDst23Even,
            new java.sql.Timestamp(EST_CONTEXT.roundWeek(lowerBoundaryWholeWeekDst23Even, 0, 1)));
        assertNotEquals(wholeWeekDst23Even,
            new java.sql.Timestamp(EST_CONTEXT.roundWeek(lowerBoundaryWholeWeekDst23Even -1, 0, 1)));
        assertEquals(wholeWeekDst23Even,
            new java.sql.Timestamp(EST_CONTEXT.roundWeek(upperBoundaryWholeWeekDst23Even, 0, 1)));
        assertNotEquals(wholeWeekDst23Even,
            new java.sql.Timestamp(EST_CONTEXT.roundWeek(upperBoundaryWholeWeekDst23Even + 1, 0, 1)));
        
        // Is it even possible for the switchover to be on an Odd week (of Year) ?

        java.sql.Timestamp wholeMonthEven =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-06-1 0:0:0").getTime());
        // May is 31 days
        long lowerBoundaryWholeMonthEven = wholeMonthEven.getTime() - 31 * HALF_DAY;
        // June is 30 days
        long upperBoundaryWholeMonthEven = wholeMonthEven.getTime() + 30 * HALF_DAY -1;
        assertEquals(lowerBoundaryWholeMonthEven, EST_CONTEXT.roundMonthLower(wholeMonthEven.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeMonthEven, EST_CONTEXT.roundMonthUpper(wholeMonthEven.getTime(), 0, 1));
        assertEquals(wholeMonthEven,
            new java.sql.Timestamp(EST_CONTEXT.roundMonth(lowerBoundaryWholeMonthEven, 0, 1)));
        assertNotEquals(wholeMonthEven,
            new java.sql.Timestamp(EST_CONTEXT.roundMonth(lowerBoundaryWholeMonthEven -1, 0, 1)));
        assertEquals(wholeMonthEven,
            new java.sql.Timestamp(EST_CONTEXT.roundMonth(upperBoundaryWholeMonthEven, 0, 1)));
        assertNotEquals(wholeMonthEven,
            new java.sql.Timestamp(EST_CONTEXT.roundMonth(upperBoundaryWholeMonthEven + 1, 0, 1)));

        java.sql.Timestamp wholeMonthOdd =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-07-1 0:0:0").getTime());
        // June is 30 days
        long lowerBoundaryWholeMonthOdd = wholeMonthOdd.getTime() - 30 * HALF_DAY;
        // July is 31 days
        long upperBoundaryWholeMonthOdd = wholeMonthOdd.getTime() + 31 * HALF_DAY - 1;
        assertEquals(lowerBoundaryWholeMonthOdd, EST_CONTEXT.roundMonthLower(wholeMonthOdd.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeMonthOdd, EST_CONTEXT.roundMonthUpper(wholeMonthOdd.getTime(), 0, 1));
        assertEquals(wholeMonthOdd,
            new java.sql.Timestamp(EST_CONTEXT.roundMonth(lowerBoundaryWholeMonthOdd, 0, 1)));
        assertNotEquals(wholeMonthOdd,
            new java.sql.Timestamp(EST_CONTEXT.roundMonth(lowerBoundaryWholeMonthOdd - 1, 0, 1)));
        assertEquals(wholeMonthOdd,
            new java.sql.Timestamp(EST_CONTEXT.roundMonth(upperBoundaryWholeMonthOdd, 0, 1)));
        assertNotEquals(wholeMonthOdd,
            new java.sql.Timestamp(EST_CONTEXT.roundMonth(upperBoundaryWholeMonthOdd + 1, 0, 1)));

        java.sql.Timestamp wholeMonthDst23Odd =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-03-1 0:0:0").getTime());
        // February is 28 days
        long lowerBoundaryWholeMonthDst23Odd = wholeMonthDst23Odd.getTime() - 28 * HALF_DAY;
        // March is 31 days
        long upperBoundaryWholeMonthDst23Odd = wholeMonthDst23Odd.getTime() + (31 * HALF_DAY - HALF_HOUR) - 1;
        assertEquals(lowerBoundaryWholeMonthDst23Odd, EST_CONTEXT.roundMonthLower(wholeMonthDst23Odd.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeMonthDst23Odd, EST_CONTEXT.roundMonthUpper(wholeMonthDst23Odd.getTime(), 0, 1));
        assertEquals(wholeMonthDst23Odd,
            new java.sql.Timestamp(EST_CONTEXT.roundMonth(lowerBoundaryWholeMonthDst23Odd, 0, 1)));
        assertNotEquals(wholeMonthDst23Odd,
            new java.sql.Timestamp(EST_CONTEXT.roundMonth(lowerBoundaryWholeMonthDst23Odd -1, 0, 1)));
        assertEquals(wholeMonthDst23Odd,
            new java.sql.Timestamp(EST_CONTEXT.roundMonth(upperBoundaryWholeMonthDst23Odd, 0, 1)));
        assertNotEquals(wholeMonthDst23Odd,
            new java.sql.Timestamp(EST_CONTEXT.roundMonth(upperBoundaryWholeMonthDst23Odd + 1, 0, 1)));

        java.sql.Timestamp wholeMonthLeap =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2024-02-1 0:0:0").getTime());
        // January is 31 days
        long lowerBoundaryWholeMonthLeap = wholeMonthLeap.getTime() - 31 * HALF_DAY;
        // February is is 29 days
        long upperBoundaryWholeMonthLeap = wholeMonthLeap.getTime() + 29 * HALF_DAY -1;
        assertEquals(lowerBoundaryWholeMonthLeap, EST_CONTEXT.roundMonthLower(wholeMonthLeap.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeMonthLeap, EST_CONTEXT.roundMonthUpper(wholeMonthLeap.getTime(), 0, 1));
        assertEquals(wholeMonthLeap,
            new java.sql.Timestamp(EST_CONTEXT.roundMonth(lowerBoundaryWholeMonthLeap, 0, 1)));
        assertNotEquals(wholeMonthLeap,
            new java.sql.Timestamp(EST_CONTEXT.roundMonth(lowerBoundaryWholeMonthLeap - 1, 0, 1)));
        assertEquals(wholeMonthLeap,
            new java.sql.Timestamp(EST_CONTEXT.roundMonth(upperBoundaryWholeMonthLeap, 0, 1)));
        assertNotEquals(wholeMonthLeap,
            new java.sql.Timestamp(EST_CONTEXT.roundMonth(upperBoundaryWholeMonthLeap + 1, 0, 1)));

        java.sql.Timestamp wholeYearEven =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-1-1 0:0:0").getTime());
        long lowerBoundaryWholeYearEven = wholeYearEven.getTime() - HALF_YEAR;
        long upperBoundaryWholeYearEven = wholeYearEven.getTime() + HALF_YEAR -1;
        assertEquals(lowerBoundaryWholeYearEven, EST_CONTEXT.roundYearLower(wholeYearEven.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeYearEven, EST_CONTEXT.roundYearUpper(wholeYearEven.getTime(), 0, 1));
        assertEquals(wholeYearEven,
            new java.sql.Timestamp(EST_CONTEXT.roundYear(lowerBoundaryWholeYearEven, 0, 1)));
        assertNotEquals(wholeYearEven,
            new java.sql.Timestamp(EST_CONTEXT.roundYear(lowerBoundaryWholeYearEven -1, 0, 1)));
        assertEquals(wholeYearEven,
            new java.sql.Timestamp(EST_CONTEXT.roundYear(upperBoundaryWholeYearEven, 0, 1)));
        assertNotEquals(wholeYearEven,
            new java.sql.Timestamp(EST_CONTEXT.roundYear(upperBoundaryWholeYearEven + 1, 0, 1)));
        
        java.sql.Timestamp wholeYearOdd =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2023-1-1 0:0:0").getTime());
        long lowerBoundaryWholeYearOdd = wholeYearOdd.getTime() - HALF_YEAR;
        long upperBoundaryWholeYearOdd = wholeYearOdd.getTime() + HALF_YEAR -1;
        assertEquals(lowerBoundaryWholeYearOdd, EST_CONTEXT.roundYearLower(wholeYearOdd.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeYearOdd, EST_CONTEXT.roundYearUpper(wholeYearOdd.getTime(), 0, 1));
        assertEquals(wholeYearOdd,
            new java.sql.Timestamp(EST_CONTEXT.roundYear(lowerBoundaryWholeYearOdd, 0, 1)));
        assertNotEquals(wholeYearOdd,
            new java.sql.Timestamp(EST_CONTEXT.roundYear(lowerBoundaryWholeYearOdd -1, 0, 1)));
        assertEquals(wholeYearOdd,
            new java.sql.Timestamp(EST_CONTEXT.roundYear(upperBoundaryWholeYearOdd, 0, 1)));
        assertNotEquals(wholeYearOdd,
            new java.sql.Timestamp(EST_CONTEXT.roundYear(upperBoundaryWholeYearOdd + 1, 0, 1)));
        
        java.sql.Timestamp wholeYearLeapEven =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2024-1-1 0:0:0").getTime());
        long lowerBoundaryWholeYearLeapEven = wholeYearLeapEven.getTime() - HALF_YEAR;
        long upperBoundaryWholeYearLeapEven = wholeYearLeapEven.getTime() + HALF_YEAR + HALF_DAY -1;
        assertEquals(lowerBoundaryWholeYearLeapEven, EST_CONTEXT.roundYearLower(wholeYearLeapEven.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeYearLeapEven, EST_CONTEXT.roundYearUpper(wholeYearLeapEven.getTime(), 0, 1));
        assertEquals(wholeYearLeapEven,
            new java.sql.Timestamp(EST_CONTEXT.roundYear(lowerBoundaryWholeYearLeapEven, 0, 1)));
        assertNotEquals(wholeYearLeapEven,
            new java.sql.Timestamp(EST_CONTEXT.roundYear(lowerBoundaryWholeYearLeapEven - 1, 0, 1)));
        assertEquals(wholeYearLeapEven,
            new java.sql.Timestamp(EST_CONTEXT.roundYear(upperBoundaryWholeYearLeapEven, 0, 1)));
        assertNotEquals(wholeYearLeapEven,
            new java.sql.Timestamp(EST_CONTEXT.roundYear(upperBoundaryWholeYearLeapEven + 1, 0, 1)));
    }

    @Test
    public void testRoundingGMT() {
        // We operate on Instants for time units up to Days, simply counting millis

        java.sql.Timestamp oddWholeSecond =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 11:11:11").getTime());
        long lowerBoundaryOddWholeSecond = oddWholeSecond.getTime() - HALF_SEC;
        long upperBoundaryOddWholeSecond = oddWholeSecond.getTime() + HALF_SEC -1;
        assertEquals(lowerBoundaryOddWholeSecond, GMT_CONTEXT.roundSecondLower(oddWholeSecond.getTime(), SEC, 1));
        assertEquals(upperBoundaryOddWholeSecond, GMT_CONTEXT.roundSecondUpper(oddWholeSecond.getTime(), SEC, 1));
        assertEquals(oddWholeSecond,
            new java.sql.Timestamp(GMT_CONTEXT.roundSecond(lowerBoundaryOddWholeSecond, SEC, 1)));
        assertNotEquals(oddWholeSecond,
            new java.sql.Timestamp(GMT_CONTEXT.roundSecond(lowerBoundaryOddWholeSecond -1, SEC, 1)));
        assertEquals(oddWholeSecond,
            new java.sql.Timestamp(GMT_CONTEXT.roundSecond(upperBoundaryOddWholeSecond, SEC, 1)));
        assertNotEquals(oddWholeSecond,
            new java.sql.Timestamp(GMT_CONTEXT.roundSecond(upperBoundaryOddWholeSecond + 1, SEC, 1)));
        
        // 10 sec range
        java.sql.Timestamp oddWholeSecondRound10 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 11:11:10").getTime());
        long lowerBoundaryOddWholeSecondRound10 = oddWholeSecondRound10.getTime() - 5 * SEC;
        long upperBoundaryOddWholeSecondRound10 = oddWholeSecondRound10.getTime() + 5 * SEC -1;
        assertEquals(lowerBoundaryOddWholeSecondRound10, EST_CONTEXT.roundSecondLower(oddWholeSecond.getTime(), 0, 10));
        assertEquals(upperBoundaryOddWholeSecondRound10, EST_CONTEXT.roundSecondUpper(oddWholeSecond.getTime(), 0, 10));
        assertEquals(oddWholeSecondRound10,
            new java.sql.Timestamp(EST_CONTEXT.roundSecond(lowerBoundaryOddWholeSecondRound10, 0, 10)));
        assertNotEquals(oddWholeSecondRound10,
            new java.sql.Timestamp(EST_CONTEXT.roundSecond(lowerBoundaryOddWholeSecondRound10 -1, 0, 10)));
        assertEquals(oddWholeSecondRound10,
            new java.sql.Timestamp(EST_CONTEXT.roundSecond(upperBoundaryOddWholeSecondRound10, 0, 10)));
        assertNotEquals(oddWholeSecondRound10,
            new java.sql.Timestamp(EST_CONTEXT.roundSecond(upperBoundaryOddWholeSecondRound10 + 1, 0, 10)));
        
        // 15 sec range
        java.sql.Timestamp oddWholeSecondRound15 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 11:11:15").getTime());
        long lowerBoundaryOddWholeSecondRound15 = oddWholeSecondRound15.getTime() - 15 * HALF_SEC;
        long upperBoundaryOddWholeSecondRound15 = oddWholeSecondRound15.getTime() + 15 * HALF_SEC -1;
        assertEquals(lowerBoundaryOddWholeSecondRound15, EST_CONTEXT.roundSecondLower(oddWholeSecond.getTime(), 0, 15));
        assertEquals(upperBoundaryOddWholeSecondRound15, EST_CONTEXT.roundSecondUpper(oddWholeSecond.getTime(), 0, 15));
        assertEquals(oddWholeSecondRound15,
            new java.sql.Timestamp(EST_CONTEXT.roundSecond(lowerBoundaryOddWholeSecondRound15, 0, 15)));
        assertNotEquals(oddWholeSecondRound15,
            new java.sql.Timestamp(EST_CONTEXT.roundSecond(lowerBoundaryOddWholeSecondRound15 -1, 0, 15)));
        assertEquals(oddWholeSecondRound15,
            new java.sql.Timestamp(EST_CONTEXT.roundSecond(upperBoundaryOddWholeSecondRound15, 0, 15)));
        assertNotEquals(oddWholeSecondRound15,
            new java.sql.Timestamp(EST_CONTEXT.roundSecond(upperBoundaryOddWholeSecondRound15 + 1, 0, 15)));
        
        java.sql.Timestamp evenWholeSecond =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 11:11:12").getTime());
        long lowerBoundaryEvenWholeSecond = evenWholeSecond.getTime() - HALF_SEC;
        long upperBoundaryEvenWholeSecond = evenWholeSecond.getTime() + HALF_SEC -1;
        assertEquals(lowerBoundaryEvenWholeSecond, GMT_CONTEXT.roundSecondLower(evenWholeSecond.getTime(), SEC, 1));
        assertEquals(upperBoundaryEvenWholeSecond, GMT_CONTEXT.roundSecondUpper(evenWholeSecond.getTime(), SEC, 1));
        assertEquals(evenWholeSecond,
            new java.sql.Timestamp(GMT_CONTEXT.roundSecond(lowerBoundaryEvenWholeSecond, SEC, 1)));
        assertNotEquals(evenWholeSecond,
            new java.sql.Timestamp(GMT_CONTEXT.roundSecond(lowerBoundaryEvenWholeSecond - 1, SEC, 1)));
        assertEquals(evenWholeSecond,
            new java.sql.Timestamp(GMT_CONTEXT.roundSecond(upperBoundaryEvenWholeSecond, SEC, 1)));
        assertNotEquals(evenWholeSecond,
            new java.sql.Timestamp(GMT_CONTEXT.roundSecond(upperBoundaryEvenWholeSecond + 1, SEC, 1)));

        java.sql.Timestamp oddWholeMinute =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 11:11:0").getTime());
        long lowerBoundaryOddWholeMinute = oddWholeMinute.getTime() - HALF_MIN;
        long upperBoundaryOddWholeMinute = oddWholeMinute.getTime() + HALF_MIN - 1;
        assertEquals(lowerBoundaryOddWholeMinute, GMT_CONTEXT.roundMinuteLower(oddWholeMinute.getTime(), MIN, 1));
        assertEquals(upperBoundaryOddWholeMinute, GMT_CONTEXT.roundMinuteUpper(oddWholeMinute.getTime(), MIN, 1));
        assertEquals(oddWholeMinute,
            new java.sql.Timestamp(GMT_CONTEXT.roundMinute(lowerBoundaryOddWholeMinute, MIN, 1)));
        assertNotEquals(oddWholeMinute,
            new java.sql.Timestamp(GMT_CONTEXT.roundMinute(lowerBoundaryOddWholeMinute -1, MIN, 1)));
        assertEquals(oddWholeMinute,
            new java.sql.Timestamp(GMT_CONTEXT.roundMinute(upperBoundaryOddWholeMinute, MIN, 1)));
        assertNotEquals(oddWholeMinute,
            new java.sql.Timestamp(GMT_CONTEXT.roundMinute(upperBoundaryOddWholeMinute + 1, MIN, 1)));
        
        java.sql.Timestamp oddWholeMinuteRound20 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 11:20:0").getTime());
        long lowerBoundaryOddWholeMinute20 = oddWholeMinuteRound20.getTime() - 10 * MIN;
        long upperBoundaryOddWholeMinute20 = oddWholeMinuteRound20.getTime() + 10 * MIN - 1;
        assertEquals(lowerBoundaryOddWholeMinute20, GMT_CONTEXT.roundMinuteLower(oddWholeMinute.getTime(), 20 * MIN, 20));
        assertEquals(upperBoundaryOddWholeMinute20, GMT_CONTEXT.roundMinuteUpper(oddWholeMinute.getTime(), 20 * MIN, 20));
        assertEquals(oddWholeMinuteRound20,
            new java.sql.Timestamp(GMT_CONTEXT.roundMinute(lowerBoundaryOddWholeMinute20, 20 * MIN, 20)));
        assertNotEquals(oddWholeMinuteRound20,
            new java.sql.Timestamp(GMT_CONTEXT.roundMinute(lowerBoundaryOddWholeMinute20 -1, 20 * MIN, 20)));
        assertEquals(oddWholeMinuteRound20,
            new java.sql.Timestamp(GMT_CONTEXT.roundMinute(upperBoundaryOddWholeMinute20, 20 * MIN, 20)));
        assertNotEquals(oddWholeMinuteRound20,
            new java.sql.Timestamp(GMT_CONTEXT.roundMinute(upperBoundaryOddWholeMinute20 + 1, 20 * MIN, 20)));
        
        //Minutes since epoch, don't expect the rounded value to be "round"
        java.sql.Timestamp oddWholeMinuteRound17 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 11:12:00").getTime());
        long lowerBoundaryOddWholeMinute17 = oddWholeMinuteRound17.getTime() - 17 * HALF_MIN;
        long upperBoundaryOddWholeMinute17 = oddWholeMinuteRound17.getTime() + 17 * HALF_MIN - 1;
        assertEquals(lowerBoundaryOddWholeMinute17, GMT_CONTEXT.roundMinuteLower(oddWholeMinute.getTime(), 17 * MIN, 17));
        assertEquals(upperBoundaryOddWholeMinute17, GMT_CONTEXT.roundMinuteUpper(oddWholeMinute.getTime(), 17 * MIN, 17));
        assertEquals(oddWholeMinuteRound17,
            new java.sql.Timestamp(GMT_CONTEXT.roundMinute(lowerBoundaryOddWholeMinute17, 17 * MIN, 17)));
        assertNotEquals(oddWholeMinuteRound17,
            new java.sql.Timestamp(GMT_CONTEXT.roundMinute(lowerBoundaryOddWholeMinute17 -1, 17 * MIN, 17)));
        assertEquals(oddWholeMinuteRound17,
            new java.sql.Timestamp(GMT_CONTEXT.roundMinute(upperBoundaryOddWholeMinute17, 17 * MIN, 17)));
        assertNotEquals(oddWholeMinuteRound17,
            new java.sql.Timestamp(GMT_CONTEXT.roundMinute(upperBoundaryOddWholeMinute17 + 1, 17 * MIN, 17)));
        
        java.sql.Timestamp evenWholeMinute =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 11:12:0").getTime());
        long lowerBoundaryEvenWholeMinute = evenWholeMinute.getTime() - HALF_MIN;
        long upperBoundaryEvenWholeMinute = evenWholeMinute.getTime() + HALF_MIN -1;
        assertEquals(lowerBoundaryEvenWholeMinute, GMT_CONTEXT.roundMinuteLower(evenWholeMinute.getTime(), MIN, 1));
        assertEquals(upperBoundaryEvenWholeMinute, GMT_CONTEXT.roundMinuteUpper(evenWholeMinute.getTime(), MIN, 1));
        assertEquals(evenWholeMinute,
            new java.sql.Timestamp(GMT_CONTEXT.roundMinute(lowerBoundaryEvenWholeMinute, MIN, 1)));
        assertNotEquals(evenWholeMinute,
            new java.sql.Timestamp(GMT_CONTEXT.roundMinute(lowerBoundaryEvenWholeMinute - 1, MIN, 1)));
        assertEquals(evenWholeMinute,
            new java.sql.Timestamp(GMT_CONTEXT.roundMinute(upperBoundaryEvenWholeMinute, MIN, 1)));
        assertNotEquals(evenWholeMinute,
            new java.sql.Timestamp(GMT_CONTEXT.roundMinute(upperBoundaryEvenWholeMinute + 1, MIN, 1)));

        java.sql.Timestamp oddWholeHour =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 11:0:0").getTime());
        long lowerBoundaryOddWholeHour = oddWholeHour.getTime() - HALF_HOUR;
        long upperBoundaryOddWholeHour = oddWholeHour.getTime() + HALF_HOUR -1;
        assertEquals(lowerBoundaryOddWholeHour, GMT_CONTEXT.roundHourLower(oddWholeHour.getTime(), HOUR, 1));
        assertEquals(upperBoundaryOddWholeHour, GMT_CONTEXT.roundHourUpper(oddWholeHour.getTime(), HOUR, 1));
        assertEquals(oddWholeHour,
            new java.sql.Timestamp(GMT_CONTEXT.roundHour(lowerBoundaryOddWholeHour, HOUR, 1)));
        assertNotEquals(oddWholeHour,
            new java.sql.Timestamp(GMT_CONTEXT.roundHour(lowerBoundaryOddWholeHour - 1, HOUR, 1)));
        assertEquals(oddWholeHour,
            new java.sql.Timestamp(GMT_CONTEXT.roundHour(upperBoundaryOddWholeHour, HOUR, 1)));
        assertNotEquals(oddWholeHour,
            new java.sql.Timestamp(GMT_CONTEXT.roundHour(upperBoundaryOddWholeHour + 1, HOUR, 1)));
        
        //Not rounding to hourOfDay
        java.sql.Timestamp oddWholeHour10 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 12:0:0").getTime());
        long lowerBoundaryOddWholeHour10 = oddWholeHour10.getTime() - HALF_HOUR * 10;
        long upperBoundaryOddWholeHour10 = oddWholeHour10.getTime() + HALF_HOUR * 10 - 1;
        assertEquals(lowerBoundaryOddWholeHour10, GMT_CONTEXT.roundHourLower(oddWholeHour.getTime(), 10 * HOUR, 10));
        assertEquals(upperBoundaryOddWholeHour10, GMT_CONTEXT.roundHourUpper(oddWholeHour.getTime(), 10 * HOUR, 10));
        assertEquals(oddWholeHour10,
            new java.sql.Timestamp(GMT_CONTEXT.roundHour(lowerBoundaryOddWholeHour10, 10 * HOUR, 10)));
        assertNotEquals(oddWholeHour10,
            new java.sql.Timestamp(GMT_CONTEXT.roundHour(lowerBoundaryOddWholeHour10 - 1, 10 * HOUR, 10)));
        assertEquals(oddWholeHour10,
            new java.sql.Timestamp(GMT_CONTEXT.roundHour(upperBoundaryOddWholeHour10, 10 * HOUR, 10)));
        assertNotEquals(oddWholeHour10,
            new java.sql.Timestamp(GMT_CONTEXT.roundHour(upperBoundaryOddWholeHour10 + 1, 10 * HOUR, 10)));
        
        //Not rounding to hourOfDay
        java.sql.Timestamp oddWholeHour11 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 07:0:0").getTime());
        long lowerBoundaryOddWholeHour11 = oddWholeHour11.getTime() - HALF_HOUR * 11;
        long upperBoundaryOddWholeHour11 = oddWholeHour11.getTime() + HALF_HOUR * 11 - 1;
        assertEquals(lowerBoundaryOddWholeHour11, GMT_CONTEXT.roundHourLower(oddWholeHour.getTime(), 11 * HOUR, 11));
        assertEquals(upperBoundaryOddWholeHour11, GMT_CONTEXT.roundHourUpper(oddWholeHour.getTime(), 11 * HOUR, 11));
        assertEquals(oddWholeHour11,
            new java.sql.Timestamp(GMT_CONTEXT.roundHour(lowerBoundaryOddWholeHour11, 11 * HOUR, 11)));
        assertNotEquals(oddWholeHour11,
            new java.sql.Timestamp(GMT_CONTEXT.roundHour(lowerBoundaryOddWholeHour11 - 1, 11 * HOUR, 11)));
        assertEquals(oddWholeHour11,
            new java.sql.Timestamp(GMT_CONTEXT.roundHour(upperBoundaryOddWholeHour11, 11 * HOUR, 11)));
        assertNotEquals(oddWholeHour11,
            new java.sql.Timestamp(GMT_CONTEXT.roundHour(upperBoundaryOddWholeHour11 + 1, 11 * HOUR, 11)));
        
        
        java.sql.Timestamp evenwholeHour =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 12:0:0").getTime());
        long lowerBoundaryEvenWholeHour = evenwholeHour.getTime() - HALF_HOUR;
        long upperBoundaryEvenWholeHour = evenwholeHour.getTime() + HALF_HOUR -1;
        assertEquals(lowerBoundaryEvenWholeHour, GMT_CONTEXT.roundHourLower(evenwholeHour.getTime(), HOUR, 1));
        assertEquals(upperBoundaryEvenWholeHour, GMT_CONTEXT.roundHourUpper(evenwholeHour.getTime(), HOUR, 1));
        assertEquals(evenwholeHour,
            new java.sql.Timestamp(GMT_CONTEXT.roundHour(lowerBoundaryEvenWholeHour, HOUR, 1)));
        assertNotEquals(evenwholeHour,
            new java.sql.Timestamp(GMT_CONTEXT.roundHour(lowerBoundaryEvenWholeHour -1, HOUR, 1)));
        assertEquals(evenwholeHour,
            new java.sql.Timestamp(GMT_CONTEXT.roundHour(upperBoundaryEvenWholeHour, HOUR, 1)));
        assertNotEquals(evenwholeHour,
            new java.sql.Timestamp(GMT_CONTEXT.roundHour(upperBoundaryEvenWholeHour + 1, HOUR, 1)));
        
        // From Days and larger periods, we're using proper date arithmetics
        // No DST switchover
        java.sql.Timestamp oddWholeDay =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 0:0:0").getTime());
        long lowerBoundaryOddWholeDay = oddWholeDay.getTime() - HALF_DAY;
        long upperBoundaryOddWholeDay = oddWholeDay.getTime() + HALF_DAY - 1;
        assertEquals(lowerBoundaryOddWholeDay, GMT_CONTEXT.roundDayLower(oddWholeDay.getTime(), DAY, 1));
        assertEquals(upperBoundaryOddWholeDay, GMT_CONTEXT.roundDayUpper(oddWholeDay.getTime(), DAY, 1));
        assertEquals(oddWholeDay,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(lowerBoundaryOddWholeDay, DAY, 1)));
        assertNotEquals(oddWholeDay,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(lowerBoundaryOddWholeDay - 1, DAY, 1)));
        assertEquals(oddWholeDay,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(upperBoundaryOddWholeDay, DAY, 1)));
        assertNotEquals(oddWholeDay,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(upperBoundaryOddWholeDay + 1, DAY, 1)));

        java.sql.Timestamp oddWholeDay10 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-14 0:0:0").getTime());
        long lowerBoundaryOddWholeDay10 = oddWholeDay10.getTime() - 10 * HALF_DAY;
        long upperBoundaryOddWholeDay10 = oddWholeDay10.getTime() + 10 * HALF_DAY - 1;
        assertEquals(lowerBoundaryOddWholeDay10, GMT_CONTEXT.roundDayLower(oddWholeDay.getTime(), 10 * DAY, 10));
        assertEquals(upperBoundaryOddWholeDay10, GMT_CONTEXT.roundDayUpper(oddWholeDay.getTime(), 10 * DAY, 10));
        assertEquals(oddWholeDay10,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(lowerBoundaryOddWholeDay10, 10 * DAY, 10)));
        assertNotEquals(oddWholeDay10,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(lowerBoundaryOddWholeDay10 - 1, 10 * DAY, 10)));
        assertEquals(oddWholeDay10,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(upperBoundaryOddWholeDay10, 10 * DAY, 10)));
        assertNotEquals(oddWholeDay10,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(upperBoundaryOddWholeDay10 + 1, 10 * DAY, 10)));

        java.sql.Timestamp oddWholeDay3 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-12 0:0:0").getTime());
        long lowerBoundaryOddWholeDay3 = oddWholeDay3.getTime() - 3 * HALF_DAY;
        long upperBoundaryOddWholeDay3 = oddWholeDay3.getTime() + 3 * HALF_DAY - 1;
        assertEquals(lowerBoundaryOddWholeDay3, GMT_CONTEXT.roundDayLower(oddWholeDay.getTime(), 3 * DAY, 3));
        assertEquals(upperBoundaryOddWholeDay3, GMT_CONTEXT.roundDayUpper(oddWholeDay.getTime(), 3 * DAY, 3));
        assertEquals(oddWholeDay3,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(lowerBoundaryOddWholeDay3, 3 * DAY, 3)));
        assertNotEquals(oddWholeDay3,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(lowerBoundaryOddWholeDay3 - 1, 3 * DAY, 3)));
        assertEquals(oddWholeDay3,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(upperBoundaryOddWholeDay3, 3 * DAY, 3)));
        assertNotEquals(oddWholeDay3,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(upperBoundaryOddWholeDay3 + 1, 3 * DAY, 3)));
        
        java.sql.Timestamp evenWholeDay =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-12 0:0:0").getTime());
        long lowerBoundaryEvenWholeDay = evenWholeDay.getTime() - HALF_DAY;
        long upperBoundaryEvenWholeDay = evenWholeDay.getTime() + HALF_DAY -1;
        assertEquals(lowerBoundaryEvenWholeDay, GMT_CONTEXT.roundDayLower(evenWholeDay.getTime(), DAY, 1));
        assertEquals(upperBoundaryEvenWholeDay, GMT_CONTEXT.roundDayUpper(evenWholeDay.getTime(), DAY, 1));
        assertEquals(evenWholeDay,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(lowerBoundaryEvenWholeDay, DAY, 1)));
        assertNotEquals(evenWholeDay,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(lowerBoundaryEvenWholeDay -1, DAY, 1)));
        assertEquals(evenWholeDay,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(upperBoundaryEvenWholeDay, DAY, 1)));
        assertNotEquals(evenWholeDay,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(upperBoundaryEvenWholeDay + 1, DAY, 1)));

        // No DST in GMT, and even if there was, we're just adding fixed ms values
        java.sql.Timestamp wholeDayDst23Odd =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-03-13 0:0:0").getTime());
        long lowerBoundaryEvenWholeDayDst23Odd = wholeDayDst23Odd.getTime() - HALF_DAY;
        long upperBoundaryEvenWholeDayDst23Odd = wholeDayDst23Odd.getTime() + HALF_DAY -1;
        assertEquals(lowerBoundaryEvenWholeDayDst23Odd, GMT_CONTEXT.roundDayLower(wholeDayDst23Odd.getTime(), DAY, 1));
        assertEquals(upperBoundaryEvenWholeDayDst23Odd, GMT_CONTEXT.roundDayUpper(wholeDayDst23Odd.getTime(), DAY, 1));
        assertEquals(wholeDayDst23Odd,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(lowerBoundaryEvenWholeDayDst23Odd, DAY, 1)));
        assertNotEquals(wholeDayDst23Odd,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(lowerBoundaryEvenWholeDayDst23Odd - 1, DAY, 1)));
        assertEquals(wholeDayDst23Odd,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(upperBoundaryEvenWholeDayDst23Odd, DAY, 1)));
        assertNotEquals(wholeDayDst23Odd,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(upperBoundaryEvenWholeDayDst23Odd + 1, DAY, 1)));
        
        //No DST in GMT
        java.sql.Timestamp wholeDayDst23Odd10 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-03-09 0:0:0").getTime());
        long lowerBoundaryEvenWholeDayDst23Odd10 = wholeDayDst23Odd10.getTime() - 10 * HALF_DAY;
        long upperBoundaryEvenWholeDayDst23Odd10 = wholeDayDst23Odd10.getTime() + (10 * HALF_DAY) - 1;
        assertEquals(lowerBoundaryEvenWholeDayDst23Odd10, GMT_CONTEXT.roundDayLower(wholeDayDst23Odd.getTime(), 10 * DAY, 10));
        assertEquals(upperBoundaryEvenWholeDayDst23Odd10, GMT_CONTEXT.roundDayUpper(wholeDayDst23Odd.getTime(), 10 * DAY, 10));
        assertEquals(wholeDayDst23Odd10,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(lowerBoundaryEvenWholeDayDst23Odd10, 10 * DAY, 10)));
        assertNotEquals(wholeDayDst23Odd10,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(lowerBoundaryEvenWholeDayDst23Odd10 - 1, 10 * DAY, 10)));
        assertEquals(wholeDayDst23Odd10,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(upperBoundaryEvenWholeDayDst23Odd10, 10 * DAY, 10)));
        assertNotEquals(wholeDayDst23Odd10,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(upperBoundaryEvenWholeDayDst23Odd10 + 1, 10 * DAY, 10)));
        
        //No DST in GMT
        java.sql.Timestamp wholeDayDst23Odd27 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-03-11 0:0:0").getTime());
        long lowerBoundaryEvenWholeDayDst23Odd27 = wholeDayDst23Odd27.getTime() - 27 * HALF_DAY;
        long upperBoundaryEvenWholeDayDst23Odd27 = wholeDayDst23Odd27.getTime() + 27 * HALF_DAY - 1;
        assertEquals(lowerBoundaryEvenWholeDayDst23Odd27, GMT_CONTEXT.roundDayLower(wholeDayDst23Odd.getTime(),  27 * DAY, 27));
        assertEquals(upperBoundaryEvenWholeDayDst23Odd27, GMT_CONTEXT.roundDayUpper(wholeDayDst23Odd.getTime(), 27 * DAY, 27));
        assertEquals(wholeDayDst23Odd27,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(lowerBoundaryEvenWholeDayDst23Odd27, 27 * DAY, 27)));
        assertNotEquals(wholeDayDst23Odd27,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(lowerBoundaryEvenWholeDayDst23Odd27 - 1, 27 * DAY, 27)));
        assertEquals(wholeDayDst23Odd27,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(upperBoundaryEvenWholeDayDst23Odd27, 27 * DAY, 27)));
        assertNotEquals(wholeDayDst23Odd27,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(upperBoundaryEvenWholeDayDst23Odd27 + 1, 27 * DAY, 27)));
        
        // No DST in GMT, and even if there was, we're just adding fixed ms values
        java.sql.Timestamp wholeDayDst23Even =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2023-03-12 0:0:0").getTime());
        long lowerBoundaryEvenWholeDayDst23Even = wholeDayDst23Even.getTime() - HALF_DAY;
        long upperBoundaryEvenWholeDayDst23Even = wholeDayDst23Even.getTime() + HALF_DAY -1;
        assertEquals(lowerBoundaryEvenWholeDayDst23Even, GMT_CONTEXT.roundDayLower(wholeDayDst23Even.getTime(), DAY, 1));
        assertEquals(upperBoundaryEvenWholeDayDst23Even, GMT_CONTEXT.roundDayUpper(wholeDayDst23Even.getTime(), DAY, 1));
        assertEquals(wholeDayDst23Even,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(lowerBoundaryEvenWholeDayDst23Even, DAY, 1)));
        assertNotEquals(wholeDayDst23Even,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(lowerBoundaryEvenWholeDayDst23Even -1, DAY, 1)));
        assertEquals(wholeDayDst23Even,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(upperBoundaryEvenWholeDayDst23Even, DAY, 1)));
        assertNotEquals(wholeDayDst23Even,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(upperBoundaryEvenWholeDayDst23Even + 1, DAY, 1)));

        // No DST in GMT, and even if there was, we're just adding fixed ms values
        java.sql.Timestamp wholeDayDst25Odd =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2023-11-05 0:0:0").getTime());
        long lowerBoundaryEvenWholeDayDst25Odd = wholeDayDst25Odd.getTime() - HALF_DAY;
        long upperBoundaryEvenWholeDayDst25Odd = wholeDayDst25Odd.getTime() + HALF_DAY -1;
        assertEquals(lowerBoundaryEvenWholeDayDst25Odd, GMT_CONTEXT.roundDayLower(wholeDayDst25Odd.getTime(), DAY, 1));
        assertEquals(upperBoundaryEvenWholeDayDst25Odd, GMT_CONTEXT.roundDayUpper(wholeDayDst25Odd.getTime(), DAY, 1));
        assertEquals(wholeDayDst25Odd,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(lowerBoundaryEvenWholeDayDst25Odd, DAY, 1)));
        assertNotEquals(wholeDayDst25Odd,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(lowerBoundaryEvenWholeDayDst25Odd -1, DAY, 1)));
        assertEquals(wholeDayDst25Odd,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(upperBoundaryEvenWholeDayDst25Odd, DAY, 1)));
        assertNotEquals(wholeDayDst25Odd,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(upperBoundaryEvenWholeDayDst25Odd + 1, DAY, 1)));

        // No DST in GMT, and even if there was, we're just adding fixed ms values
        java.sql.Timestamp wholeDayDst25Even =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-06 0:0:0").getTime());
        long lowerBoundaryEvenWholeDayDst25Even = wholeDayDst25Even.getTime() - HALF_DAY;
        long upperBoundaryEvenWholeDayDst25Even = wholeDayDst25Even.getTime() + HALF_DAY -1;
        assertEquals(lowerBoundaryEvenWholeDayDst25Even, GMT_CONTEXT.roundDayLower(wholeDayDst25Even.getTime(), DAY, 1));
        assertEquals(upperBoundaryEvenWholeDayDst25Even, GMT_CONTEXT.roundDayUpper(wholeDayDst25Even.getTime(), DAY, 1));
        assertEquals(wholeDayDst25Even,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(lowerBoundaryEvenWholeDayDst25Even, DAY, 1)));
        assertNotEquals(wholeDayDst25Even,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(lowerBoundaryEvenWholeDayDst25Even -1, DAY, 1)));
        assertEquals(wholeDayDst25Even,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(upperBoundaryEvenWholeDayDst25Even, DAY, 1)));
        assertNotEquals(wholeDayDst25Even,
            new java.sql.Timestamp(GMT_CONTEXT.roundDay(upperBoundaryEvenWholeDayDst25Even + 1, DAY, 1)));

        java.sql.Timestamp wholeWeekOdd =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-10-10 0:0:0").getTime());
        long lowerBoundaryWholeWeekOdd = wholeWeekOdd.getTime() - (HALF_WEEK - 1);
        long upperBoundaryWholeWeekOdd = wholeWeekOdd.getTime() + HALF_WEEK - 1;
        assertEquals(lowerBoundaryWholeWeekOdd, GMT_CONTEXT.roundWeekLower(wholeWeekOdd.getTime(), DAY, 1));
        assertEquals(upperBoundaryWholeWeekOdd, GMT_CONTEXT.roundWeekUpper(wholeWeekOdd.getTime(), DAY, 1));
        assertEquals(wholeWeekOdd,
            new java.sql.Timestamp(GMT_CONTEXT.roundWeek(lowerBoundaryWholeWeekOdd, DAY, 1)));
        assertNotEquals(wholeWeekOdd,
            new java.sql.Timestamp(GMT_CONTEXT.roundWeek(lowerBoundaryWholeWeekOdd -1, DAY, 1)));
        assertEquals(wholeWeekOdd,
            new java.sql.Timestamp(GMT_CONTEXT.roundWeek(upperBoundaryWholeWeekOdd, DAY, 1)));
        assertNotEquals(wholeWeekOdd,
            new java.sql.Timestamp(GMT_CONTEXT.roundWeek(upperBoundaryWholeWeekOdd + 1, DAY, 1)));

        java.sql.Timestamp wholeWeekEven =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-10-17 0:0:0").getTime());
        long lowerBoundaryWholeWeekEven = wholeWeekEven.getTime() - HALF_WEEK;
        long upperBoundaryWholeWeekEven = wholeWeekEven.getTime() + HALF_WEEK;
        assertEquals(lowerBoundaryWholeWeekEven, GMT_CONTEXT.roundWeekLower(wholeWeekEven.getTime(), DAY, 1));
        assertEquals(upperBoundaryWholeWeekEven, GMT_CONTEXT.roundWeekUpper(wholeWeekEven.getTime(), DAY, 1));
        assertEquals(wholeWeekEven,
            new java.sql.Timestamp(GMT_CONTEXT.roundWeek(lowerBoundaryWholeWeekEven, DAY, 1)));
        assertNotEquals(wholeWeekEven,
            new java.sql.Timestamp(GMT_CONTEXT.roundWeek(lowerBoundaryWholeWeekEven - 1, DAY, 1)));
        assertEquals(wholeWeekEven,
            new java.sql.Timestamp(GMT_CONTEXT.roundWeek(upperBoundaryWholeWeekEven, DAY, 1)));
        assertNotEquals(wholeWeekEven,
            new java.sql.Timestamp(GMT_CONTEXT.roundWeek(upperBoundaryWholeWeekEven + 1, DAY, 1)));

        // No DST in GMT
        // We're still using roundHalfEven here for backwards compatibility
        java.sql.Timestamp wholeWeekDst25Even =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-10-31 0:0:0").getTime());
        long lowerBoundaryWholeWeekDst25Even = wholeWeekDst25Even.getTime() - HALF_WEEK;
        long upperBoundaryWholeWeekDst25Even = wholeWeekDst25Even.getTime() + HALF_WEEK;
        assertEquals(lowerBoundaryWholeWeekDst25Even, GMT_CONTEXT.roundWeekLower(wholeWeekDst25Even.getTime(), WEEK, 1));
        assertEquals(upperBoundaryWholeWeekDst25Even, GMT_CONTEXT.roundWeekUpper(wholeWeekDst25Even.getTime(), WEEK, 1));
        assertEquals(wholeWeekDst25Even,
            new java.sql.Timestamp(GMT_CONTEXT.roundWeek(lowerBoundaryWholeWeekDst25Even, WEEK, 1)));
        assertNotEquals(wholeWeekDst25Even,
            new java.sql.Timestamp(GMT_CONTEXT.roundWeek(lowerBoundaryWholeWeekDst25Even - 1, WEEK, 1)));
        assertEquals(wholeWeekDst25Even,
            new java.sql.Timestamp(GMT_CONTEXT.roundWeek(upperBoundaryWholeWeekDst25Even, WEEK, 1)));
        assertNotEquals(wholeWeekDst25Even,
            new java.sql.Timestamp(GMT_CONTEXT.roundWeek(upperBoundaryWholeWeekDst25Even + 1, WEEK, 1)));

        // Is it even possible for the switchover to be on an Odd week (of Year) ?
        
        // No DST in GMT
        // We're still using roundHalfEven here for backwards compatibility
        java.sql.Timestamp wholeWeekDst23Even =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-03-07 0:0:0").getTime());
        long lowerBoundaryWholeWeekDst23Even = wholeWeekDst23Even.getTime() - HALF_WEEK;
        long upperBoundaryWholeWeekDst23Even = wholeWeekDst23Even.getTime() + HALF_WEEK;
        assertEquals(lowerBoundaryWholeWeekDst23Even, GMT_CONTEXT.roundWeekLower(wholeWeekDst23Even.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeWeekDst23Even, GMT_CONTEXT.roundWeekUpper(wholeWeekDst23Even.getTime(), 0, 1));
        assertEquals(wholeWeekDst23Even,
            new java.sql.Timestamp(GMT_CONTEXT.roundWeek(lowerBoundaryWholeWeekDst23Even, 0, 1)));
        assertNotEquals(wholeWeekDst23Even,
            new java.sql.Timestamp(GMT_CONTEXT.roundWeek(lowerBoundaryWholeWeekDst23Even -1, 0, 1)));
        assertEquals(wholeWeekDst23Even,
            new java.sql.Timestamp(GMT_CONTEXT.roundWeek(upperBoundaryWholeWeekDst23Even, 0, 1)));
        assertNotEquals(wholeWeekDst23Even,
            new java.sql.Timestamp(GMT_CONTEXT.roundWeek(upperBoundaryWholeWeekDst23Even + 1, 0, 1)));
        
        // Is it even possible for the switchover to be on an Odd week (of Year) ?

        // We're still using roundHalfEven here for backwards compatibility
        java.sql.Timestamp wholeMonthEven =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-06-1 0:0:0").getTime());
        // May is 31 days
        long lowerBoundaryWholeMonthEven = wholeMonthEven.getTime() - 31 * HALF_DAY;
        // June is 30 days
        long upperBoundaryWholeMonthEven = wholeMonthEven.getTime() + 30 * HALF_DAY;
        assertEquals(lowerBoundaryWholeMonthEven, GMT_CONTEXT.roundMonthLower(wholeMonthEven.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeMonthEven, GMT_CONTEXT.roundMonthUpper(wholeMonthEven.getTime(), 0, 1));
        assertEquals(wholeMonthEven,
            new java.sql.Timestamp(GMT_CONTEXT.roundMonth(lowerBoundaryWholeMonthEven, 0, 1)));
        assertNotEquals(wholeMonthEven,
            new java.sql.Timestamp(GMT_CONTEXT.roundMonth(lowerBoundaryWholeMonthEven -1, 0, 1)));
        assertEquals(wholeMonthEven,
            new java.sql.Timestamp(GMT_CONTEXT.roundMonth(upperBoundaryWholeMonthEven, 0, 1)));
        assertNotEquals(wholeMonthEven,
            new java.sql.Timestamp(GMT_CONTEXT.roundMonth(upperBoundaryWholeMonthEven + 1, 0, 1)));

        // We're still using roundHalfEven here for backwards compatibility
        java.sql.Timestamp wholeMonthOdd =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-07-1 0:0:0").getTime());
        // June is 30 days
        long lowerBoundaryWholeMonthOdd = wholeMonthOdd.getTime() - 30 * HALF_DAY + 1;
        // July is 31 days
        long upperBoundaryWholeMonthOdd = wholeMonthOdd.getTime() + 31 * HALF_DAY - 1;
        assertEquals(lowerBoundaryWholeMonthOdd, GMT_CONTEXT.roundMonthLower(wholeMonthOdd.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeMonthOdd, GMT_CONTEXT.roundMonthUpper(wholeMonthOdd.getTime(), 0, 1));
        assertEquals(wholeMonthOdd,
            new java.sql.Timestamp(GMT_CONTEXT.roundMonth(lowerBoundaryWholeMonthOdd, 0, 1)));
        assertNotEquals(wholeMonthOdd,
            new java.sql.Timestamp(GMT_CONTEXT.roundMonth(lowerBoundaryWholeMonthOdd - 1, 0, 1)));
        assertEquals(wholeMonthOdd,
            new java.sql.Timestamp(GMT_CONTEXT.roundMonth(upperBoundaryWholeMonthOdd, 0, 1)));
        assertNotEquals(wholeMonthOdd,
            new java.sql.Timestamp(GMT_CONTEXT.roundMonth(upperBoundaryWholeMonthOdd + 1, 0, 1)));

        // No DST in GMT
        // We're still using roundHalfEven here for backwards compatibility
        java.sql.Timestamp wholeMonthDst23Odd =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-03-1 0:0:0").getTime());
        // February is 28 days
        long lowerBoundaryWholeMonthDst23Odd = wholeMonthDst23Odd.getTime() - 28 * HALF_DAY + 1;
        // March is 31 days
        long upperBoundaryWholeMonthDst23Odd = wholeMonthDst23Odd.getTime() + 31 * HALF_DAY - 1;
        assertEquals(lowerBoundaryWholeMonthDst23Odd, GMT_CONTEXT.roundMonthLower(wholeMonthDst23Odd.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeMonthDst23Odd, GMT_CONTEXT.roundMonthUpper(wholeMonthDst23Odd.getTime(), 0, 1));
        assertEquals(wholeMonthDst23Odd,
            new java.sql.Timestamp(GMT_CONTEXT.roundMonth(lowerBoundaryWholeMonthDst23Odd, 0, 1)));
        assertNotEquals(wholeMonthDst23Odd,
            new java.sql.Timestamp(GMT_CONTEXT.roundMonth(lowerBoundaryWholeMonthDst23Odd -1, 0, 1)));
        assertEquals(wholeMonthDst23Odd,
            new java.sql.Timestamp(GMT_CONTEXT.roundMonth(upperBoundaryWholeMonthDst23Odd, 0, 1)));
        assertNotEquals(wholeMonthDst23Odd,
            new java.sql.Timestamp(GMT_CONTEXT.roundMonth(upperBoundaryWholeMonthDst23Odd + 1, 0, 1)));

        // We're still using roundHalfEven here for backwards compatibility
        java.sql.Timestamp wholeMonthLeap =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2024-02-1 0:0:0").getTime());
        // January is 31 days
        long lowerBoundaryWholeMonthLeap = wholeMonthLeap.getTime() - 31 * HALF_DAY;
        // February is is 29 days
        long upperBoundaryWholeMonthLeap = wholeMonthLeap.getTime() + 29 * HALF_DAY;
        assertEquals(lowerBoundaryWholeMonthLeap, GMT_CONTEXT.roundMonthLower(wholeMonthLeap.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeMonthLeap, GMT_CONTEXT.roundMonthUpper(wholeMonthLeap.getTime(), 0, 1));
        assertEquals(wholeMonthLeap,
            new java.sql.Timestamp(GMT_CONTEXT.roundMonth(lowerBoundaryWholeMonthLeap, 0, 1)));
        assertNotEquals(wholeMonthLeap,
            new java.sql.Timestamp(GMT_CONTEXT.roundMonth(lowerBoundaryWholeMonthLeap - 1, 0, 1)));
        assertEquals(wholeMonthLeap,
            new java.sql.Timestamp(GMT_CONTEXT.roundMonth(upperBoundaryWholeMonthLeap, 0, 1)));
        assertNotEquals(wholeMonthLeap,
            new java.sql.Timestamp(GMT_CONTEXT.roundMonth(upperBoundaryWholeMonthLeap + 1, 0, 1)));

        // We're still using roundHalfEven here for backwards compatibility
        java.sql.Timestamp wholeYearEven =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-1-1 0:0:0").getTime());
        long lowerBoundaryWholeYearEven = wholeYearEven.getTime() - HALF_YEAR;
        long upperBoundaryWholeYearEven = wholeYearEven.getTime() + HALF_YEAR;
        assertEquals(lowerBoundaryWholeYearEven, GMT_CONTEXT.roundYearLower(wholeYearEven.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeYearEven, GMT_CONTEXT.roundYearUpper(wholeYearEven.getTime(), 0, 1));
        assertEquals(wholeYearEven,
            new java.sql.Timestamp(GMT_CONTEXT.roundYear(lowerBoundaryWholeYearEven, 0, 1)));
        assertNotEquals(wholeYearEven,
            new java.sql.Timestamp(GMT_CONTEXT.roundYear(lowerBoundaryWholeYearEven -1, 0, 1)));
        assertEquals(wholeYearEven,
            new java.sql.Timestamp(GMT_CONTEXT.roundYear(upperBoundaryWholeYearEven, 0, 1)));
        assertNotEquals(wholeYearEven,
            new java.sql.Timestamp(GMT_CONTEXT.roundYear(upperBoundaryWholeYearEven + 1, 0, 1)));
        
        // We're still using roundHalfEven here for backwards compatibility
        java.sql.Timestamp wholeYearOdd =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2023-1-1 0:0:0").getTime());
        long lowerBoundaryWholeYearOdd = wholeYearOdd.getTime() - HALF_YEAR + 1;
        long upperBoundaryWholeYearOdd = wholeYearOdd.getTime() + HALF_YEAR - 1;
        assertEquals(lowerBoundaryWholeYearOdd, GMT_CONTEXT.roundYearLower(wholeYearOdd.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeYearOdd, GMT_CONTEXT.roundYearUpper(wholeYearOdd.getTime(), 0, 1));
        assertEquals(wholeYearOdd,
            new java.sql.Timestamp(GMT_CONTEXT.roundYear(lowerBoundaryWholeYearOdd, 0, 1)));
        assertNotEquals(wholeYearOdd,
            new java.sql.Timestamp(GMT_CONTEXT.roundYear(lowerBoundaryWholeYearOdd -1, 0, 1)));
        assertEquals(wholeYearOdd,
            new java.sql.Timestamp(GMT_CONTEXT.roundYear(upperBoundaryWholeYearOdd, 0, 1)));
        assertNotEquals(wholeYearOdd,
            new java.sql.Timestamp(GMT_CONTEXT.roundYear(upperBoundaryWholeYearOdd + 1, 0, 1)));
        
        // We're still using roundHalfEven here for backwards compatibility
        java.sql.Timestamp wholeYearLeapEven =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2024-1-1 0:0:0").getTime());
        long lowerBoundaryWholeYearLeapEven = wholeYearLeapEven.getTime() - HALF_YEAR;
        long upperBoundaryWholeYearLeapEven = wholeYearLeapEven.getTime() + HALF_YEAR + HALF_DAY;
        assertEquals(lowerBoundaryWholeYearLeapEven, GMT_CONTEXT.roundYearLower(wholeYearLeapEven.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeYearLeapEven, GMT_CONTEXT.roundYearUpper(wholeYearLeapEven.getTime(), 0, 1));
        assertEquals(wholeYearLeapEven,
            new java.sql.Timestamp(GMT_CONTEXT.roundYear(lowerBoundaryWholeYearLeapEven, 0, 1)));
        assertNotEquals(wholeYearLeapEven,
            new java.sql.Timestamp(GMT_CONTEXT.roundYear(lowerBoundaryWholeYearLeapEven - 1, 0, 1)));
        assertEquals(wholeYearLeapEven,
            new java.sql.Timestamp(GMT_CONTEXT.roundYear(upperBoundaryWholeYearLeapEven, 0, 1)));
        assertNotEquals(wholeYearLeapEven,
            new java.sql.Timestamp(GMT_CONTEXT.roundYear(upperBoundaryWholeYearLeapEven + 1, 0, 1)));
    }
    
    @Test
    public void testFloorCompliant() {
        // We operate on Instants for time units up to Days, simply counting millis
        
        // No need to repeat odd / even cases
        // The logic for upper and lower scan ranges is in the Expression classes. It is always
        // [floor(ts), ceil(ts+1)-1]
        
        java.sql.Timestamp oddWholeSecond =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 11:11:11").getTime());
        long lowerBoundaryOddWholeSecond = oddWholeSecond.getTime();
        long upperBoundaryOddWholeSecond = oddWholeSecond.getTime() + SEC -1;
        assertEquals(lowerBoundaryOddWholeSecond, EST_CONTEXT.floorSecond(oddWholeSecond.getTime(), 0, 1));
        assertEquals(upperBoundaryOddWholeSecond, EST_CONTEXT.ceilSecond(oddWholeSecond.getTime() + 1, 0, 1) - 1);
        assertEquals(oddWholeSecond,
            new java.sql.Timestamp(EST_CONTEXT.floorSecond(lowerBoundaryOddWholeSecond, 0, 1)));
        assertNotEquals(oddWholeSecond,
            new java.sql.Timestamp(EST_CONTEXT.floorSecond(lowerBoundaryOddWholeSecond -1, 0, 1)));
        assertEquals(oddWholeSecond,
            new java.sql.Timestamp(EST_CONTEXT.floorSecond(upperBoundaryOddWholeSecond, 0, 1)));
        assertNotEquals(oddWholeSecond,
            new java.sql.Timestamp(EST_CONTEXT.floorSecond(upperBoundaryOddWholeSecond + 1, 0, 1)));

        
        // 10 sec range
        java.sql.Timestamp oddWholeSecondFloor10 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 11:11:10").getTime());
        long lowerBoundaryOddWholeSecondFloor10 = oddWholeSecondFloor10.getTime() ;
        long upperBoundaryOddWholeSecondFloor10 = oddWholeSecondFloor10.getTime() + 10 * SEC -1;
        assertEquals(lowerBoundaryOddWholeSecondFloor10, EST_CONTEXT.floorSecond(oddWholeSecond.getTime(), 0, 10));
        assertEquals(upperBoundaryOddWholeSecondFloor10, EST_CONTEXT.ceilSecond(oddWholeSecond.getTime() + 1, 0, 10) - 1);
        assertEquals(oddWholeSecondFloor10,
            new java.sql.Timestamp(EST_CONTEXT.floorSecond(lowerBoundaryOddWholeSecondFloor10, 0, 10)));
        assertNotEquals(oddWholeSecondFloor10,
            new java.sql.Timestamp(EST_CONTEXT.floorSecond(lowerBoundaryOddWholeSecondFloor10 -1, 0, 10)));
        assertEquals(oddWholeSecondFloor10,
            new java.sql.Timestamp(EST_CONTEXT.floorSecond(upperBoundaryOddWholeSecondFloor10, 0, 10)));
        assertNotEquals(oddWholeSecondFloor10,
            new java.sql.Timestamp(EST_CONTEXT.floorSecond(upperBoundaryOddWholeSecondFloor10 + 1, 0, 10)));
        
        // 15 sec range
        java.sql.Timestamp oddWholeSecondFloor15 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 11:11:0").getTime());
        long lowerBoundaryOddWholeSecondFloor15 = oddWholeSecondFloor15.getTime();
        long upperBoundaryOddWholeSecondFloor15 = oddWholeSecondFloor15.getTime() + 15 * SEC -1;
        assertEquals(lowerBoundaryOddWholeSecondFloor15, EST_CONTEXT.floorSecond(oddWholeSecond.getTime(), 0, 15));
        assertEquals(upperBoundaryOddWholeSecondFloor15, EST_CONTEXT.ceilSecond(oddWholeSecond.getTime() + 1, 0, 15) -1);
        assertEquals(oddWholeSecondFloor15,
            new java.sql.Timestamp(EST_CONTEXT.floorSecond(lowerBoundaryOddWholeSecondFloor15, 0, 15)));
        assertNotEquals(oddWholeSecondFloor15,
            new java.sql.Timestamp(EST_CONTEXT.floorSecond(lowerBoundaryOddWholeSecondFloor15 -1, 0, 15)));
        assertEquals(oddWholeSecondFloor15,
            new java.sql.Timestamp(EST_CONTEXT.floorSecond(upperBoundaryOddWholeSecondFloor15, 0, 15)));
        assertNotEquals(oddWholeSecondFloor15,
            new java.sql.Timestamp(EST_CONTEXT.floorSecond(upperBoundaryOddWholeSecondFloor15 + 1, 0, 15)));
        
        java.sql.Timestamp evenWholeMinute =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 11:12:0").getTime());
        long lowerBoundaryEvenWholeMinute = evenWholeMinute.getTime();
        long upperBoundaryEvenWholeMinute = evenWholeMinute.getTime() + MIN -1;
        assertEquals(lowerBoundaryEvenWholeMinute, EST_CONTEXT.floorMinute(evenWholeMinute.getTime(), 0, 1));
        assertEquals(upperBoundaryEvenWholeMinute, EST_CONTEXT.ceilMinute(evenWholeMinute.getTime() + 1, 0, 1) -1);
        assertEquals(evenWholeMinute,
            new java.sql.Timestamp(EST_CONTEXT.floorMinute(lowerBoundaryEvenWholeMinute, 0, 1)));
        assertNotEquals(evenWholeMinute,
            new java.sql.Timestamp(EST_CONTEXT.floorMinute(lowerBoundaryEvenWholeMinute - 1, 0, 1)));
        assertEquals(evenWholeMinute,
            new java.sql.Timestamp(EST_CONTEXT.floorMinute(upperBoundaryEvenWholeMinute, 0, 1)));
        assertNotEquals(evenWholeMinute,
            new java.sql.Timestamp(EST_CONTEXT.floorMinute(upperBoundaryEvenWholeMinute + 1, 0, 1)));

        java.sql.Timestamp evenWholeMinuteFloor20 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 11:00:0").getTime());
        long lowerBoundaryEvenWholeMinuteFloor20 = evenWholeMinuteFloor20.getTime();
        long upperBoundaryEvenWholeMinuteFloor20 = evenWholeMinuteFloor20.getTime() + 20 * MIN - 1;
        assertEquals(lowerBoundaryEvenWholeMinuteFloor20, EST_CONTEXT.floorMinute(evenWholeMinute.getTime(), 0, 20));
        assertEquals(upperBoundaryEvenWholeMinuteFloor20, EST_CONTEXT.ceilMinute(evenWholeMinute.getTime() + 1, 0, 20) - 1);
        assertEquals(evenWholeMinuteFloor20,
            new java.sql.Timestamp(EST_CONTEXT.floorMinute(lowerBoundaryEvenWholeMinuteFloor20, 0, 20)));
        assertNotEquals(evenWholeMinuteFloor20,
            new java.sql.Timestamp(EST_CONTEXT.floorMinute(lowerBoundaryEvenWholeMinuteFloor20 -1, 0, 20)));
        assertEquals(evenWholeMinuteFloor20,
            new java.sql.Timestamp(EST_CONTEXT.floorMinute(upperBoundaryEvenWholeMinuteFloor20, 0, 20)));
        assertNotEquals(evenWholeMinuteFloor20,
            new java.sql.Timestamp(EST_CONTEXT.floorMinute(upperBoundaryEvenWholeMinuteFloor20 + 1, 0, 20)));
        
        //Minutes since epoch, don't expect the rounded value to be "round"
        java.sql.Timestamp evenWholeMinuteFloor17 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 11:12:00").getTime());
        long lowerBoundaryEvenWholeMinute17 = evenWholeMinuteFloor17.getTime();
        long upperBoundaryEvenWholeMinute17 = evenWholeMinuteFloor17.getTime() + 17 * MIN - 1;
        assertEquals(lowerBoundaryEvenWholeMinute17, EST_CONTEXT.floorMinute(evenWholeMinute.getTime(), 0, 17));
        assertEquals(upperBoundaryEvenWholeMinute17, EST_CONTEXT.ceilMinute(evenWholeMinute.getTime() + 1, 0, 17) - 1);
        assertEquals(evenWholeMinuteFloor17,
            new java.sql.Timestamp(EST_CONTEXT.floorMinute(lowerBoundaryEvenWholeMinute17, 0, 17)));
        assertNotEquals(evenWholeMinuteFloor17,
            new java.sql.Timestamp(EST_CONTEXT.floorMinute(lowerBoundaryEvenWholeMinute17 -1, 0, 17)));
        assertEquals(evenWholeMinuteFloor17,
            new java.sql.Timestamp(EST_CONTEXT.floorMinute(upperBoundaryEvenWholeMinute17, 0, 17)));
        assertNotEquals(evenWholeMinuteFloor17,
            new java.sql.Timestamp(EST_CONTEXT.floorMinute(upperBoundaryEvenWholeMinute17 + 1, 0, 17)));
        
        
        java.sql.Timestamp oddWholeHour =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 11:0:0").getTime());
        long lowerBoundaryOddWholeHour = oddWholeHour.getTime();
        long upperBoundaryOddWholeHour = oddWholeHour.getTime() + HOUR -1;
        assertEquals(lowerBoundaryOddWholeHour, EST_CONTEXT.floorHour(oddWholeHour.getTime(), 0, 1));
        assertEquals(upperBoundaryOddWholeHour, EST_CONTEXT.ceilHour(oddWholeHour.getTime() + 1, 0, 1) -1);
        assertEquals(oddWholeHour,
            new java.sql.Timestamp(EST_CONTEXT.floorHour(lowerBoundaryOddWholeHour, 0, 1)));
        assertNotEquals(oddWholeHour,
            new java.sql.Timestamp(EST_CONTEXT.floorHour(lowerBoundaryOddWholeHour - 1, 0, 1)));
        assertEquals(oddWholeHour,
            new java.sql.Timestamp(EST_CONTEXT.floorHour(upperBoundaryOddWholeHour, 0, 1)));
        assertNotEquals(oddWholeHour,
            new java.sql.Timestamp(EST_CONTEXT.floorHour(upperBoundaryOddWholeHour + 1, 0, 1)));

        //Not rounding to hourOfDay
        java.sql.Timestamp oddWholeHour10 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 02:0:0").getTime());
        long lowerBoundaryOddWholeHour10 = oddWholeHour10.getTime();
        long upperBoundaryOddWholeHour10 = oddWholeHour10.getTime() + HOUR * 10 - 1;
        assertEquals(lowerBoundaryOddWholeHour10, EST_CONTEXT.floorHour(oddWholeHour.getTime(), 0, 10));
        assertEquals(upperBoundaryOddWholeHour10, EST_CONTEXT.ceilHour(oddWholeHour.getTime() + 1, 0, 10) - 1);
        assertEquals(oddWholeHour10,
            new java.sql.Timestamp(EST_CONTEXT.floorHour(lowerBoundaryOddWholeHour10, 0, 10)));
        assertNotEquals(oddWholeHour10,
            new java.sql.Timestamp(EST_CONTEXT.floorHour(lowerBoundaryOddWholeHour10 - 1, 0, 10)));
        assertEquals(oddWholeHour10,
            new java.sql.Timestamp(EST_CONTEXT.floorHour(upperBoundaryOddWholeHour10, 0, 10)));
        assertNotEquals(oddWholeHour10,
            new java.sql.Timestamp(EST_CONTEXT.floorHour(upperBoundaryOddWholeHour10 + 1, 0, 10)));
        
        //Not rounding to hourOfDay
        java.sql.Timestamp oddWholeHour11 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 07:0:0").getTime());
        long lowerBoundaryOddWholeHour11 = oddWholeHour11.getTime();
        long upperBoundaryOddWholeHour11 = oddWholeHour11.getTime() + HOUR * 11 - 1;
        assertEquals(lowerBoundaryOddWholeHour11, EST_CONTEXT.floorHour(oddWholeHour.getTime(), 0, 11));
        assertEquals(upperBoundaryOddWholeHour11, EST_CONTEXT.ceilHour(oddWholeHour.getTime() + 1, 0, 11) - 1);
        assertEquals(oddWholeHour11,
            new java.sql.Timestamp(EST_CONTEXT.floorHour(lowerBoundaryOddWholeHour11, 0, 11)));
        assertNotEquals(oddWholeHour11,
            new java.sql.Timestamp(EST_CONTEXT.floorHour(lowerBoundaryOddWholeHour11 - 1, 0, 11)));
        assertEquals(oddWholeHour11,
            new java.sql.Timestamp(EST_CONTEXT.floorHour(upperBoundaryOddWholeHour11, 0, 11)));
        assertNotEquals(oddWholeHour11,
            new java.sql.Timestamp(EST_CONTEXT.floorHour(upperBoundaryOddWholeHour11 + 1, 0, 11)));

        // From Days and larger periods, we're using proper date arithmetics 
        // No DST switchover
        java.sql.Timestamp evenWholeDay =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-12 0:0:0").getTime());
        long lowerBoundaryEvenWholeDay = evenWholeDay.getTime();
        long upperBoundaryEvenWholeDay = evenWholeDay.getTime() + DAY - 1;
        assertEquals(lowerBoundaryEvenWholeDay, EST_CONTEXT.floorDay(evenWholeDay.getTime(), 0, 1));
        assertEquals(upperBoundaryEvenWholeDay, EST_CONTEXT.ceilDay(evenWholeDay.getTime() + 1, 0, 1) -1);
        assertEquals(evenWholeDay,
            new java.sql.Timestamp(EST_CONTEXT.floorDay(lowerBoundaryEvenWholeDay, 0, 1)));
        assertNotEquals(evenWholeDay,
            new java.sql.Timestamp(EST_CONTEXT.floorDay(lowerBoundaryEvenWholeDay -1, 0, 1)));
        assertEquals(evenWholeDay,
            new java.sql.Timestamp(EST_CONTEXT.floorDay(upperBoundaryEvenWholeDay, 0, 1)));
        assertNotEquals(evenWholeDay,
            new java.sql.Timestamp(EST_CONTEXT.floorDay(upperBoundaryEvenWholeDay + 1, 0, 1)));

        java.sql.Timestamp evenWholeDay2 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-12 0:0:0").getTime());
        long lowerBoundaryEvenWholeDay2 = evenWholeDay2.getTime();
        long upperBoundaryEvenWholeDay2 = evenWholeDay2.getTime() + 2 * DAY - 1;
        assertEquals(lowerBoundaryEvenWholeDay2, EST_CONTEXT.floorDay(evenWholeDay.getTime(), 0, 2));
        assertEquals(upperBoundaryEvenWholeDay2, EST_CONTEXT.ceilDay(evenWholeDay.getTime() + 1, 0, 2) - 1);
        assertEquals(evenWholeDay2,
            new java.sql.Timestamp(EST_CONTEXT.floorDay(lowerBoundaryEvenWholeDay2, 0, 2)));
        assertNotEquals(evenWholeDay2,
            new java.sql.Timestamp(EST_CONTEXT.floorDay(lowerBoundaryEvenWholeDay2 - 1, 0, 2)));
        assertEquals(evenWholeDay2,
            new java.sql.Timestamp(EST_CONTEXT.floorDay(upperBoundaryEvenWholeDay2, 0, 2)));
        assertNotEquals(evenWholeDay2,
            new java.sql.Timestamp(EST_CONTEXT.floorDay(upperBoundaryEvenWholeDay2 + 1, 0, 2)));

        java.sql.Timestamp evenWholeDay3 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-12 0:0:0").getTime());
        long lowerBoundaryEvenWholeDay3 = evenWholeDay3.getTime();
        long upperBoundaryEvenWholeDay3 = evenWholeDay3.getTime() + 3 * DAY - 1;
        assertEquals(lowerBoundaryEvenWholeDay3, EST_CONTEXT.floorDay(evenWholeDay.getTime(), 0, 3));
        assertEquals(upperBoundaryEvenWholeDay3, EST_CONTEXT.ceilDay(evenWholeDay.getTime() + 1, 0, 3) - 1);
        assertEquals(evenWholeDay3,
            new java.sql.Timestamp(EST_CONTEXT.floorDay(lowerBoundaryEvenWholeDay3, 0, 3)));
        assertNotEquals(evenWholeDay3,
            new java.sql.Timestamp(EST_CONTEXT.floorDay(lowerBoundaryEvenWholeDay3 - 1, 0, 3)));
        assertEquals(evenWholeDay3,
            new java.sql.Timestamp(EST_CONTEXT.floorDay(upperBoundaryEvenWholeDay3, 0, 3)));
        assertNotEquals(evenWholeDay3,
            new java.sql.Timestamp(EST_CONTEXT.floorDay(upperBoundaryEvenWholeDay3 + 1, 0, 3)));
        
        java.sql.Timestamp wholeDayDst23Odd =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-03-13 0:0:0").getTime());
        java.sql.Timestamp wholeDayDst23Odd10 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-03-09 0:0:0").getTime());
        long lowerBoundaryEvenWholeDayDst23Odd10 = wholeDayDst23Odd10.getTime();
        long upperBoundaryEvenWholeDayDst23Odd10 = wholeDayDst23Odd10.getTime() + (10 * DAY - HOUR) - 1;
        assertEquals(lowerBoundaryEvenWholeDayDst23Odd10, EST_CONTEXT.floorDay(wholeDayDst23Odd.getTime(), 0, 10));
        assertEquals(upperBoundaryEvenWholeDayDst23Odd10, EST_CONTEXT.ceilDay(wholeDayDst23Odd.getTime() + 1, 0, 10) - 1);
        assertEquals(wholeDayDst23Odd10,
            new java.sql.Timestamp(EST_CONTEXT.floorDay(lowerBoundaryEvenWholeDayDst23Odd10, 0, 10)));
        assertNotEquals(wholeDayDst23Odd10,
            new java.sql.Timestamp(EST_CONTEXT.floorDay(lowerBoundaryEvenWholeDayDst23Odd10 - 1, 0, 10)));
        assertEquals(wholeDayDst23Odd10,
            new java.sql.Timestamp(EST_CONTEXT.floorDay(upperBoundaryEvenWholeDayDst23Odd10, 0, 10)));
        assertNotEquals(wholeDayDst23Odd10,
            new java.sql.Timestamp(EST_CONTEXT.floorDay(upperBoundaryEvenWholeDayDst23Odd10 + 1, 0, 10)));
        
        java.sql.Timestamp wholeDayDst23Odd27 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-03-11 0:0:0").getTime());
        long lowerBoundaryEvenWholeDayDst23Odd27 = wholeDayDst23Odd27.getTime();
        long upperBoundaryEvenWholeDayDst23Odd27 = wholeDayDst23Odd27.getTime() + (27 * DAY - HOUR) - 1;
        assertEquals(lowerBoundaryEvenWholeDayDst23Odd27, EST_CONTEXT.floorDay(wholeDayDst23Odd.getTime(), 0, 27));
        assertEquals(upperBoundaryEvenWholeDayDst23Odd27, EST_CONTEXT.ceilDay(wholeDayDst23Odd.getTime() + 1, 0, 27) - 1);
        assertEquals(wholeDayDst23Odd27,
            new java.sql.Timestamp(EST_CONTEXT.floorDay(lowerBoundaryEvenWholeDayDst23Odd27, 0, 27)));
        assertNotEquals(wholeDayDst23Odd27,
            new java.sql.Timestamp(EST_CONTEXT.floorDay(lowerBoundaryEvenWholeDayDst23Odd27 - 1, 0, 27)));
        assertEquals(wholeDayDst23Odd27,
            new java.sql.Timestamp(EST_CONTEXT.floorDay(upperBoundaryEvenWholeDayDst23Odd27, 0, 27)));
        assertNotEquals(wholeDayDst23Odd27,
            new java.sql.Timestamp(EST_CONTEXT.floorDay(upperBoundaryEvenWholeDayDst23Odd27 + 1, 0, 27)));

        //DST switchover day (6th is day is 25 hours long)
        java.sql.Timestamp wholeDayDst25Even =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-06 0:0:0").getTime());
        long lowerBoundaryEvenWholeDayDst25Even = wholeDayDst25Even.getTime();
        long upperBoundaryEvenWholeDayDst25Even = wholeDayDst25Even.getTime() + DAY + HOUR -1;
        assertEquals(lowerBoundaryEvenWholeDayDst25Even, EST_CONTEXT.floorDay(wholeDayDst25Even.getTime(), 0, 1));
        assertEquals(upperBoundaryEvenWholeDayDst25Even, EST_CONTEXT.ceilDay(wholeDayDst25Even.getTime() + 1, 0, 1) -1);
        assertEquals(wholeDayDst25Even,
            new java.sql.Timestamp(EST_CONTEXT.floorDay(lowerBoundaryEvenWholeDayDst25Even, 0, 1)));
        assertNotEquals(wholeDayDst25Even,
            new java.sql.Timestamp(EST_CONTEXT.floorDay(lowerBoundaryEvenWholeDayDst25Even -1, 0, 1)));
        assertEquals(wholeDayDst25Even,
            new java.sql.Timestamp(EST_CONTEXT.floorDay(upperBoundaryEvenWholeDayDst25Even, 0, 1)));
        assertNotEquals(wholeDayDst25Even,
            new java.sql.Timestamp(EST_CONTEXT.floorDay(upperBoundaryEvenWholeDayDst25Even + 1, 0, 1)));

        java.sql.Timestamp wholeWeekOdd =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-10-10 0:0:0").getTime());
        long lowerBoundaryWholeWeekOdd = wholeWeekOdd.getTime();
        long upperBoundaryWholeWeekOdd = wholeWeekOdd.getTime() + WEEK - 1;
        assertEquals(lowerBoundaryWholeWeekOdd, EST_CONTEXT.floorWeek(wholeWeekOdd.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeWeekOdd, EST_CONTEXT.ceilWeek(wholeWeekOdd.getTime() + 1, 0, 1) -1);
        assertEquals(wholeWeekOdd,
            new java.sql.Timestamp(EST_CONTEXT.floorWeek(lowerBoundaryWholeWeekOdd, 0, 1)));
        assertNotEquals(wholeWeekOdd,
            new java.sql.Timestamp(EST_CONTEXT.floorWeek(lowerBoundaryWholeWeekOdd -1, 0, 1)));
        assertEquals(wholeWeekOdd,
            new java.sql.Timestamp(EST_CONTEXT.floorWeek(upperBoundaryWholeWeekOdd, 0, 1)));
        assertNotEquals(wholeWeekOdd,
            new java.sql.Timestamp(EST_CONTEXT.floorWeek(upperBoundaryWholeWeekOdd + 1, 0, 1)));

        java.sql.Timestamp wholeWeekDst25Even =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-10-31 0:0:0").getTime());
        long lowerBoundaryWholeWeekDst25Even = wholeWeekDst25Even.getTime();
        long upperBoundaryWholeWeekDst25Even = wholeWeekDst25Even.getTime() + WEEK + HOUR -1;
        assertEquals(lowerBoundaryWholeWeekDst25Even, EST_CONTEXT.floorWeek(wholeWeekDst25Even.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeWeekDst25Even, EST_CONTEXT.ceilWeek(wholeWeekDst25Even.getTime() +1, 0, 1) -1);
        assertEquals(wholeWeekDst25Even,
            new java.sql.Timestamp(EST_CONTEXT.floorWeek(lowerBoundaryWholeWeekDst25Even, 0, 1)));
        assertNotEquals(wholeWeekDst25Even,
            new java.sql.Timestamp(EST_CONTEXT.floorWeek(lowerBoundaryWholeWeekDst25Even - 1, 0, 1)));
        assertEquals(wholeWeekDst25Even,
            new java.sql.Timestamp(EST_CONTEXT.floorWeek(upperBoundaryWholeWeekDst25Even, 0, 1)));
        assertNotEquals(wholeWeekDst25Even,
            new java.sql.Timestamp(EST_CONTEXT.floorWeek(upperBoundaryWholeWeekDst25Even + 1, 0, 1)));

        // Is it even possible for the switchover to be on an Odd week (of Year) ?
        
        java.sql.Timestamp wholeWeekDst23Even =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-03-07 0:0:0").getTime());
        long lowerBoundaryWholeWeekDst23Even = wholeWeekDst23Even.getTime();
        long upperBoundaryWholeWeekDst23Even = wholeWeekDst23Even.getTime() + WEEK - HOUR - 1;
        assertEquals(lowerBoundaryWholeWeekDst23Even, EST_CONTEXT.floorWeek(wholeWeekDst23Even.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeWeekDst23Even, EST_CONTEXT.ceilWeek(wholeWeekDst23Even.getTime() + 1, 0, 1) -1);
        assertEquals(wholeWeekDst23Even,
            new java.sql.Timestamp(EST_CONTEXT.floorWeek(lowerBoundaryWholeWeekDst23Even, 0, 1)));
        assertNotEquals(wholeWeekDst23Even,
            new java.sql.Timestamp(EST_CONTEXT.floorWeek(lowerBoundaryWholeWeekDst23Even -1, 0, 1)));
        assertEquals(wholeWeekDst23Even,
            new java.sql.Timestamp(EST_CONTEXT.floorWeek(upperBoundaryWholeWeekDst23Even, 0, 1)));
        assertNotEquals(wholeWeekDst23Even,
            new java.sql.Timestamp(EST_CONTEXT.floorWeek(upperBoundaryWholeWeekDst23Even + 1, 0, 1)));
        
        // Is it even possible for the switchover to be on an Odd week (of Year) ?

        java.sql.Timestamp wholeMonthOdd =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-07-1 0:0:0").getTime());
        long lowerBoundaryWholeMonthOdd = wholeMonthOdd.getTime();
        // July is 31 days
        long upperBoundaryWholeMonthOdd = wholeMonthOdd.getTime() + 31 * DAY - 1;
        assertEquals(lowerBoundaryWholeMonthOdd, EST_CONTEXT.floorMonth(wholeMonthOdd.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeMonthOdd, EST_CONTEXT.ceilMonth(wholeMonthOdd.getTime() + 1, 0, 1) -1);
        assertEquals(wholeMonthOdd,
            new java.sql.Timestamp(EST_CONTEXT.floorMonth(lowerBoundaryWholeMonthOdd, 0, 1)));
        assertNotEquals(wholeMonthOdd,
            new java.sql.Timestamp(EST_CONTEXT.floorMonth(lowerBoundaryWholeMonthOdd - 1, 0, 1)));
        assertEquals(wholeMonthOdd,
            new java.sql.Timestamp(EST_CONTEXT.floorMonth(upperBoundaryWholeMonthOdd, 0, 1)));
        assertNotEquals(wholeMonthOdd,
            new java.sql.Timestamp(EST_CONTEXT.floorMonth(upperBoundaryWholeMonthOdd + 1, 0, 1)));

        java.sql.Timestamp wholeMonthDst23Odd =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-03-1 0:0:0").getTime());
        long lowerBoundaryWholeMonthDst23Odd = wholeMonthDst23Odd.getTime();
        // March is 31 days
        long upperBoundaryWholeMonthDst23Odd = wholeMonthDst23Odd.getTime() + 31 * DAY - HOUR - 1;
        assertEquals(lowerBoundaryWholeMonthDst23Odd, EST_CONTEXT.floorMonth(wholeMonthDst23Odd.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeMonthDst23Odd, EST_CONTEXT.ceilMonth(wholeMonthDst23Odd.getTime() + 1, 0, 1) -1);
        assertEquals(wholeMonthDst23Odd,
            new java.sql.Timestamp(EST_CONTEXT.floorMonth(lowerBoundaryWholeMonthDst23Odd, 0, 1)));
        assertNotEquals(wholeMonthDst23Odd,
            new java.sql.Timestamp(EST_CONTEXT.floorMonth(lowerBoundaryWholeMonthDst23Odd -1, 0, 1)));
        assertEquals(wholeMonthDst23Odd,
            new java.sql.Timestamp(EST_CONTEXT.floorMonth(upperBoundaryWholeMonthDst23Odd, 0, 1)));
        assertNotEquals(wholeMonthDst23Odd,
            new java.sql.Timestamp(EST_CONTEXT.floorMonth(upperBoundaryWholeMonthDst23Odd + 1, 0, 1)));

        java.sql.Timestamp wholeMonthLeap =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2024-02-1 0:0:0").getTime());
        long lowerBoundaryWholeMonthLeap = wholeMonthLeap.getTime();
        // February is is 29 days
        long upperBoundaryWholeMonthLeap = wholeMonthLeap.getTime() + 29 * DAY -1;
        assertEquals(lowerBoundaryWholeMonthLeap, EST_CONTEXT.floorMonth(wholeMonthLeap.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeMonthLeap, EST_CONTEXT.ceilMonth(wholeMonthLeap.getTime() +1, 0, 1) - 1);
        assertEquals(wholeMonthLeap,
            new java.sql.Timestamp(EST_CONTEXT.floorMonth(lowerBoundaryWholeMonthLeap, 0, 1)));
        assertNotEquals(wholeMonthLeap,
            new java.sql.Timestamp(EST_CONTEXT.floorMonth(lowerBoundaryWholeMonthLeap - 1, 0, 1)));
        assertEquals(wholeMonthLeap,
            new java.sql.Timestamp(EST_CONTEXT.floorMonth(upperBoundaryWholeMonthLeap, 0, 1)));
        assertNotEquals(wholeMonthLeap,
            new java.sql.Timestamp(EST_CONTEXT.floorMonth(upperBoundaryWholeMonthLeap + 1, 0, 1)));

        java.sql.Timestamp wholeYearEven =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-1-1 0:0:0").getTime());
        long lowerBoundaryWholeYearEven = wholeYearEven.getTime();
        long upperBoundaryWholeYearEven = wholeYearEven.getTime() + YEAR -1;
        assertEquals(lowerBoundaryWholeYearEven, EST_CONTEXT.floorYear(wholeYearEven.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeYearEven, EST_CONTEXT.ceilYear(wholeYearEven.getTime() + 1, 0, 1) - 1);
        assertEquals(wholeYearEven,
            new java.sql.Timestamp(EST_CONTEXT.floorYear(lowerBoundaryWholeYearEven, 0, 1)));
        assertNotEquals(wholeYearEven,
            new java.sql.Timestamp(EST_CONTEXT.floorYear(lowerBoundaryWholeYearEven -1, 0, 1)));
        assertEquals(wholeYearEven,
            new java.sql.Timestamp(EST_CONTEXT.floorYear(upperBoundaryWholeYearEven, 0, 1)));
        assertNotEquals(wholeYearEven,
            new java.sql.Timestamp(EST_CONTEXT.floorYear(upperBoundaryWholeYearEven + 1, 0, 1)));

        java.sql.Timestamp wholeYearLeapEven =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2024-1-1 0:0:0").getTime());
        long lowerBoundaryWholeYearLeapEven = wholeYearLeapEven.getTime();
        long upperBoundaryWholeYearLeapEven = wholeYearLeapEven.getTime() + YEAR + DAY - 1;
        assertEquals(lowerBoundaryWholeYearLeapEven, EST_CONTEXT.floorYear(wholeYearLeapEven.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeYearLeapEven, EST_CONTEXT.ceilYear(wholeYearLeapEven.getTime() + 1, 0, 1) -1);
        assertEquals(wholeYearLeapEven,
            new java.sql.Timestamp(EST_CONTEXT.floorYear(lowerBoundaryWholeYearLeapEven, 0, 1)));
        assertNotEquals(wholeYearLeapEven,
            new java.sql.Timestamp(EST_CONTEXT.floorYear(lowerBoundaryWholeYearLeapEven - 1, 0, 1)));
        assertEquals(wholeYearLeapEven,
            new java.sql.Timestamp(EST_CONTEXT.floorYear(upperBoundaryWholeYearLeapEven, 0, 1)));
        assertNotEquals(wholeYearLeapEven,
            new java.sql.Timestamp(EST_CONTEXT.floorYear(upperBoundaryWholeYearLeapEven + 1, 0, 1)));
    }

    @Test
    public void testFloorGMT() {
        // We operate on Instants for time units up to Days, simply counting millis
        
        // No need to repeat odd / even cases
        // The logic for upper and lower scan ranges is in the Expression classes. It is always
        // [floor(ts), ceil(ts+1)-1]
        
        java.sql.Timestamp oddWholeSecond =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 11:11:11").getTime());
        long lowerBoundaryOddWholeSecond = oddWholeSecond.getTime();
        long upperBoundaryOddWholeSecond = oddWholeSecond.getTime() + SEC -1;
        assertEquals(lowerBoundaryOddWholeSecond, GMT_CONTEXT.floorSecond(oddWholeSecond.getTime(), SEC, 1));
        assertEquals(upperBoundaryOddWholeSecond, GMT_CONTEXT.ceilSecond(oddWholeSecond.getTime() + 1, SEC, 1) -1);
        assertEquals(oddWholeSecond,
            new java.sql.Timestamp(GMT_CONTEXT.floorSecond(lowerBoundaryOddWholeSecond, SEC, 1)));
        assertNotEquals(oddWholeSecond,
            new java.sql.Timestamp(GMT_CONTEXT.floorSecond(lowerBoundaryOddWholeSecond -1, SEC, 1)));
        assertEquals(oddWholeSecond,
            new java.sql.Timestamp(GMT_CONTEXT.floorSecond(upperBoundaryOddWholeSecond, SEC, 1)));
        assertNotEquals(oddWholeSecond,
            new java.sql.Timestamp(GMT_CONTEXT.floorSecond(upperBoundaryOddWholeSecond + 1, SEC, 1)));

        // 10 sec range
        java.sql.Timestamp oddWholeSecondFloor10 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 11:11:10").getTime());
        long lowerBoundaryOddWholeSecondFloor10 = oddWholeSecondFloor10.getTime();
        long upperBoundaryOddWholeSecondFloor10 = oddWholeSecondFloor10.getTime() + 10 * SEC -1;
        assertEquals(lowerBoundaryOddWholeSecondFloor10, GMT_CONTEXT.floorSecond(oddWholeSecond.getTime(), 10 * SEC, 10));
        assertEquals(upperBoundaryOddWholeSecondFloor10, GMT_CONTEXT.ceilSecond(oddWholeSecond.getTime() + 1, 10 * SEC, 10) - 1);
        assertEquals(oddWholeSecondFloor10,
            new java.sql.Timestamp(GMT_CONTEXT.floorSecond(lowerBoundaryOddWholeSecondFloor10, 10 * SEC, 10)));
        assertNotEquals(oddWholeSecondFloor10,
            new java.sql.Timestamp(GMT_CONTEXT.floorSecond(lowerBoundaryOddWholeSecondFloor10 -1, 10 * SEC, 10)));
        assertEquals(oddWholeSecondFloor10,
            new java.sql.Timestamp(GMT_CONTEXT.floorSecond(upperBoundaryOddWholeSecondFloor10, 10 * SEC, 10)));
        assertNotEquals(oddWholeSecondFloor10,
            new java.sql.Timestamp(GMT_CONTEXT.floorSecond(upperBoundaryOddWholeSecondFloor10 + 1, 10 * SEC, 10)));
        
        // 15 sec range
        java.sql.Timestamp oddWholeSecondFloor15 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 11:11:0").getTime());
        long lowerBoundaryOddWholeSecondFloor15 = oddWholeSecondFloor15.getTime();
        long upperBoundaryOddWholeSecondFloor15 = oddWholeSecondFloor15.getTime() + 15 * SEC -1;
        assertEquals(lowerBoundaryOddWholeSecondFloor15, GMT_CONTEXT.floorSecond(oddWholeSecond.getTime(), 15 * SEC, 15));
        assertEquals(upperBoundaryOddWholeSecondFloor15, GMT_CONTEXT.ceilSecond(oddWholeSecond.getTime() + 1, 15 * SEC, 15) -1);
        assertEquals(oddWholeSecondFloor15,
            new java.sql.Timestamp(GMT_CONTEXT.floorSecond(lowerBoundaryOddWholeSecondFloor15, 15 * SEC, 15)));
        assertNotEquals(oddWholeSecondFloor15,
            new java.sql.Timestamp(GMT_CONTEXT.floorSecond(lowerBoundaryOddWholeSecondFloor15 -1, 15 * SEC, 15)));
        assertEquals(oddWholeSecondFloor15,
            new java.sql.Timestamp(GMT_CONTEXT.floorSecond(upperBoundaryOddWholeSecondFloor15, 15 * SEC, 15)));
        assertNotEquals(oddWholeSecondFloor15,
            new java.sql.Timestamp(GMT_CONTEXT.floorSecond(upperBoundaryOddWholeSecondFloor15 + 1, 15 * SEC, 15)));
        
        
        java.sql.Timestamp evenWholeMinute =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 11:12:0").getTime());
        long lowerBoundaryEvenWholeMinute = evenWholeMinute.getTime();
        long upperBoundaryEvenWholeMinute = evenWholeMinute.getTime() + MIN -1;
        assertEquals(lowerBoundaryEvenWholeMinute, GMT_CONTEXT.floorMinute(evenWholeMinute.getTime(), MIN, 1));
        assertEquals(upperBoundaryEvenWholeMinute, GMT_CONTEXT.ceilMinute(evenWholeMinute.getTime() + 1, MIN, 1) -1);
        assertEquals(evenWholeMinute,
            new java.sql.Timestamp(GMT_CONTEXT.floorMinute(lowerBoundaryEvenWholeMinute, MIN, 1)));
        assertNotEquals(evenWholeMinute,
            new java.sql.Timestamp(GMT_CONTEXT.floorMinute(lowerBoundaryEvenWholeMinute - 1, MIN, 1)));
        assertEquals(evenWholeMinute,
            new java.sql.Timestamp(GMT_CONTEXT.floorMinute(upperBoundaryEvenWholeMinute, MIN, 1)));
        assertNotEquals(evenWholeMinute,
            new java.sql.Timestamp(GMT_CONTEXT.floorMinute(upperBoundaryEvenWholeMinute + 1, MIN, 1)));

        java.sql.Timestamp evenWholeMinuteFloor20 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 11:00:0").getTime());
        long lowerBoundaryEvenWholeMinuteFloor20 = evenWholeMinuteFloor20.getTime();
        long upperBoundaryEvenWholeMinuteFloor20 = evenWholeMinuteFloor20.getTime() + 20 * MIN - 1;
        assertEquals(lowerBoundaryEvenWholeMinuteFloor20, GMT_CONTEXT.floorMinute(evenWholeMinute.getTime(), 20 * MIN, 20));
        assertEquals(upperBoundaryEvenWholeMinuteFloor20, GMT_CONTEXT.ceilMinute(evenWholeMinute.getTime() + 1, 20 * MIN, 20) - 1);
        assertEquals(evenWholeMinuteFloor20,
            new java.sql.Timestamp(GMT_CONTEXT.floorMinute(lowerBoundaryEvenWholeMinuteFloor20, 20 * MIN, 20)));
        assertNotEquals(evenWholeMinuteFloor20,
            new java.sql.Timestamp(GMT_CONTEXT.floorMinute(lowerBoundaryEvenWholeMinuteFloor20 -1, 20 * MIN, 20)));
        assertEquals(evenWholeMinuteFloor20,
            new java.sql.Timestamp(GMT_CONTEXT.floorMinute(upperBoundaryEvenWholeMinuteFloor20, 20 * MIN, 20)));
        assertNotEquals(evenWholeMinuteFloor20,
            new java.sql.Timestamp(GMT_CONTEXT.floorMinute(upperBoundaryEvenWholeMinuteFloor20 + 1, 20 * MIN, 20)));
        
        //Minutes since epoch, don't expect the rounded value to be "round"
        java.sql.Timestamp evenWholeMinuteFloor17 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 11:12:00").getTime());
        long lowerBoundaryEvenWholeMinute17 = evenWholeMinuteFloor17.getTime();
        long upperBoundaryEvenWholeMinute17 = evenWholeMinuteFloor17.getTime() + 17 * MIN - 1;
        assertEquals(lowerBoundaryEvenWholeMinute17, GMT_CONTEXT.floorMinute(evenWholeMinute.getTime(), 17 * MIN, 17));
        assertEquals(upperBoundaryEvenWholeMinute17, GMT_CONTEXT.ceilMinute(evenWholeMinute.getTime() + 1, 17 * MIN, 17) - 1);
        assertEquals(evenWholeMinuteFloor17,
            new java.sql.Timestamp(GMT_CONTEXT.floorMinute(lowerBoundaryEvenWholeMinute17, 17 * MIN, 17)));
        assertNotEquals(evenWholeMinuteFloor17,
            new java.sql.Timestamp(GMT_CONTEXT.floorMinute(lowerBoundaryEvenWholeMinute17 -1, 17 * MIN, 17)));
        assertEquals(evenWholeMinuteFloor17,
            new java.sql.Timestamp(GMT_CONTEXT.floorMinute(upperBoundaryEvenWholeMinute17, 17 * MIN, 17)));
        assertNotEquals(evenWholeMinuteFloor17,
            new java.sql.Timestamp(GMT_CONTEXT.floorMinute(upperBoundaryEvenWholeMinute17 + 1, 17 * MIN, 17)));
        
        java.sql.Timestamp oddWholeHour =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 11:0:0").getTime());
        long lowerBoundaryOddWholeHour = oddWholeHour.getTime();
        long upperBoundaryOddWholeHour = oddWholeHour.getTime() + HOUR -1;
        assertEquals(lowerBoundaryOddWholeHour, GMT_CONTEXT.floorHour(oddWholeHour.getTime(), HOUR, 1));
        assertEquals(upperBoundaryOddWholeHour, GMT_CONTEXT.ceilHour(oddWholeHour.getTime() + 1, HOUR, 1) -1);
        assertEquals(oddWholeHour,
            new java.sql.Timestamp(GMT_CONTEXT.floorHour(lowerBoundaryOddWholeHour, HOUR, 1)));
        assertNotEquals(oddWholeHour,
            new java.sql.Timestamp(GMT_CONTEXT.floorHour(lowerBoundaryOddWholeHour - 1, HOUR, 1)));
        assertEquals(oddWholeHour,
            new java.sql.Timestamp(GMT_CONTEXT.floorHour(upperBoundaryOddWholeHour, HOUR, 1)));
        assertNotEquals(oddWholeHour,
            new java.sql.Timestamp(GMT_CONTEXT.floorHour(upperBoundaryOddWholeHour + 1, HOUR, 1)));

        //Not rounding to hourOfDay
        java.sql.Timestamp oddWholeHour10 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 02:0:0").getTime());
        long lowerBoundaryOddWholeHour10 = oddWholeHour10.getTime();
        long upperBoundaryOddWholeHour10 = oddWholeHour10.getTime() + HOUR * 10 - 1;
        assertEquals(lowerBoundaryOddWholeHour10, GMT_CONTEXT.floorHour(oddWholeHour.getTime(), 10 * HOUR, 10));
        assertEquals(upperBoundaryOddWholeHour10, GMT_CONTEXT.ceilHour(oddWholeHour.getTime() + 1, 10 * HOUR, 10) - 1);
        assertEquals(oddWholeHour10,
            new java.sql.Timestamp(GMT_CONTEXT.floorHour(lowerBoundaryOddWholeHour10, 10 * HOUR, 10)));
        assertNotEquals(oddWholeHour10,
            new java.sql.Timestamp(GMT_CONTEXT.floorHour(lowerBoundaryOddWholeHour10 - 1, 10 * HOUR, 10)));
        assertEquals(oddWholeHour10,
            new java.sql.Timestamp(GMT_CONTEXT.floorHour(upperBoundaryOddWholeHour10, 10 * HOUR, 10)));
        assertNotEquals(oddWholeHour10,
            new java.sql.Timestamp(GMT_CONTEXT.floorHour(upperBoundaryOddWholeHour10 + 1, 10  *HOUR, 10)));
        
        //Not rounding to hourOfDay
        java.sql.Timestamp oddWholeHour11 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 07:0:0").getTime());
        long lowerBoundaryOddWholeHour11 = oddWholeHour11.getTime();
        long upperBoundaryOddWholeHour11 = oddWholeHour11.getTime() + HOUR * 11 - 1;
        assertEquals(lowerBoundaryOddWholeHour11, GMT_CONTEXT.floorHour(oddWholeHour.getTime(), 11 * HOUR, 11));
        assertEquals(upperBoundaryOddWholeHour11, GMT_CONTEXT.ceilHour(oddWholeHour.getTime() + 1, 11 * HOUR, 11) - 1);
        assertEquals(oddWholeHour11,
            new java.sql.Timestamp(GMT_CONTEXT.floorHour(lowerBoundaryOddWholeHour11, 11 * HOUR, 11)));
        assertNotEquals(oddWholeHour11,
            new java.sql.Timestamp(GMT_CONTEXT.floorHour(lowerBoundaryOddWholeHour11 - 1, 11 * HOUR, 11)));
        assertEquals(oddWholeHour11,
            new java.sql.Timestamp(GMT_CONTEXT.floorHour(upperBoundaryOddWholeHour11, 11 * HOUR, 11)));
        assertNotEquals(oddWholeHour11,
            new java.sql.Timestamp(GMT_CONTEXT.floorHour(upperBoundaryOddWholeHour11 + 1, 11 * HOUR, 11)));
        
        // From Days and larger periods, we're using proper date arithmetics 
        // No DST switchover
        java.sql.Timestamp evenWholeDay =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-12 0:0:0").getTime());
        long lowerBoundaryEvenWholeDay = evenWholeDay.getTime();
        long upperBoundaryEvenWholeDay = evenWholeDay.getTime() + DAY - 1;
        assertEquals(lowerBoundaryEvenWholeDay, GMT_CONTEXT.floorDay(evenWholeDay.getTime(), DAY, 1));
        assertEquals(upperBoundaryEvenWholeDay, GMT_CONTEXT.ceilDay(evenWholeDay.getTime() + 1, DAY, 1) -1);
        assertEquals(evenWholeDay,
            new java.sql.Timestamp(GMT_CONTEXT.floorDay(lowerBoundaryEvenWholeDay, DAY, 1)));
        assertNotEquals(evenWholeDay,
            new java.sql.Timestamp(GMT_CONTEXT.floorDay(lowerBoundaryEvenWholeDay -1, DAY, 1)));
        assertEquals(evenWholeDay,
            new java.sql.Timestamp(GMT_CONTEXT.floorDay(upperBoundaryEvenWholeDay, DAY, 1)));
        assertNotEquals(evenWholeDay,
            new java.sql.Timestamp(GMT_CONTEXT.floorDay(upperBoundaryEvenWholeDay + 1, DAY, 1)));

        java.sql.Timestamp evenWholeDay2 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-12 0:0:0").getTime());
        long lowerBoundaryEvenWholeDay2 = evenWholeDay2.getTime();
        long upperBoundaryEvenWholeDay2 = evenWholeDay2.getTime() + 2 * DAY - 1;
        assertEquals(lowerBoundaryEvenWholeDay2, GMT_CONTEXT.floorDay(evenWholeDay.getTime(), 2 * DAY, 2));
        assertEquals(upperBoundaryEvenWholeDay2, GMT_CONTEXT.ceilDay(evenWholeDay.getTime() + 1, 2 * DAY, 2) - 1);
        assertEquals(evenWholeDay2,
            new java.sql.Timestamp(GMT_CONTEXT.floorDay(lowerBoundaryEvenWholeDay2, 2 * DAY, 2)));
        assertNotEquals(evenWholeDay2,
            new java.sql.Timestamp(GMT_CONTEXT.floorDay(lowerBoundaryEvenWholeDay2 - 1, 2 * DAY, 2)));
        assertEquals(evenWholeDay2,
            new java.sql.Timestamp(GMT_CONTEXT.floorDay(upperBoundaryEvenWholeDay2, 2 * DAY, 2)));
        assertNotEquals(evenWholeDay2,
            new java.sql.Timestamp(GMT_CONTEXT.floorDay(upperBoundaryEvenWholeDay2 + 1, 2 * DAY, 2)));

        java.sql.Timestamp evenWholeDay3 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-12 0:0:0").getTime());
        long lowerBoundaryEvenWholeDay3 = evenWholeDay3.getTime();
        long upperBoundaryEvenWholeDay3 = evenWholeDay3.getTime() + 3 * DAY - 1;
        assertEquals(lowerBoundaryEvenWholeDay3, GMT_CONTEXT.floorDay(evenWholeDay.getTime(), 3 * DAY, 3));
        assertEquals(upperBoundaryEvenWholeDay3, GMT_CONTEXT.ceilDay(evenWholeDay.getTime() + 1, 3 * DAY, 3) - 1);
        assertEquals(evenWholeDay3,
            new java.sql.Timestamp(GMT_CONTEXT.floorDay(lowerBoundaryEvenWholeDay3, 3 * DAY, 3)));
        assertNotEquals(evenWholeDay3,
            new java.sql.Timestamp(GMT_CONTEXT.floorDay(lowerBoundaryEvenWholeDay3 - 1, 3 * DAY, 3)));
        assertEquals(evenWholeDay3,
            new java.sql.Timestamp(GMT_CONTEXT.floorDay(upperBoundaryEvenWholeDay3, 3 * DAY, 3)));
        assertNotEquals(evenWholeDay3,
            new java.sql.Timestamp(GMT_CONTEXT.floorDay(upperBoundaryEvenWholeDay3 + 1, 3 * DAY, 3)));
        
        //NO DST in GMT
        java.sql.Timestamp wholeDayDst23Odd =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-03-13 0:0:0").getTime());
        java.sql.Timestamp wholeDayDst23Odd10 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-03-09 0:0:0").getTime());
        long lowerBoundaryEvenWholeDayDst23Odd10 = wholeDayDst23Odd10.getTime();
        long upperBoundaryEvenWholeDayDst23Odd10 = wholeDayDst23Odd10.getTime() + (10 * DAY) - 1;
        assertEquals(lowerBoundaryEvenWholeDayDst23Odd10, GMT_CONTEXT.floorDay(wholeDayDst23Odd.getTime(), 10 * DAY, 10));
        assertEquals(upperBoundaryEvenWholeDayDst23Odd10, GMT_CONTEXT.ceilDay(wholeDayDst23Odd.getTime() + 1, 10 * DAY, 10) - 1);
        assertEquals(wholeDayDst23Odd10,
            new java.sql.Timestamp(GMT_CONTEXT.floorDay(lowerBoundaryEvenWholeDayDst23Odd10, 10 * DAY, 10)));
        assertNotEquals(wholeDayDst23Odd10,
            new java.sql.Timestamp(GMT_CONTEXT.floorDay(lowerBoundaryEvenWholeDayDst23Odd10 - 1, 10 * DAY, 10)));
        assertEquals(wholeDayDst23Odd10,
            new java.sql.Timestamp(GMT_CONTEXT.floorDay(upperBoundaryEvenWholeDayDst23Odd10, 10 * DAY, 10)));
        assertNotEquals(wholeDayDst23Odd10,
            new java.sql.Timestamp(GMT_CONTEXT.floorDay(upperBoundaryEvenWholeDayDst23Odd10 + 1, 10 * DAY, 10)));
        
        //NO DST in GMT
        java.sql.Timestamp wholeDayDst23Odd27 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-03-11 0:0:0").getTime());
        long lowerBoundaryEvenWholeDayDst23Odd27 = wholeDayDst23Odd27.getTime();
        long upperBoundaryEvenWholeDayDst23Odd27 = wholeDayDst23Odd27.getTime() + (27 * DAY) - 1;
        assertEquals(lowerBoundaryEvenWholeDayDst23Odd27, GMT_CONTEXT.floorDay(wholeDayDst23Odd.getTime(), 27 * DAY, 27));
        assertEquals(upperBoundaryEvenWholeDayDst23Odd27, GMT_CONTEXT.ceilDay(wholeDayDst23Odd.getTime() + 1, 27 * DAY, 27) - 1);
        assertEquals(wholeDayDst23Odd27,
            new java.sql.Timestamp(GMT_CONTEXT.floorDay(lowerBoundaryEvenWholeDayDst23Odd27, 27 * DAY, 27)));
        assertNotEquals(wholeDayDst23Odd27,
            new java.sql.Timestamp(GMT_CONTEXT.floorDay(lowerBoundaryEvenWholeDayDst23Odd27 - 1, 27 * DAY, 27)));
        assertEquals(wholeDayDst23Odd27,
            new java.sql.Timestamp(GMT_CONTEXT.floorDay(upperBoundaryEvenWholeDayDst23Odd27, 27 * DAY, 27)));
        assertNotEquals(wholeDayDst23Odd27,
            new java.sql.Timestamp(GMT_CONTEXT.floorDay(upperBoundaryEvenWholeDayDst23Odd27 + 1, 27 * DAY, 27)));
        
        //DST switchover day (13th is 23 hours long)
        // No DST in GMT
        java.sql.Timestamp wholeDayDst23Even =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2023-03-12 0:0:0").getTime());
        long lowerBoundaryEvenWholeDayDst23Even = wholeDayDst23Even.getTime();
        long upperBoundaryEvenWholeDayDst23Even = wholeDayDst23Even.getTime() + DAY - 1;
        assertEquals(lowerBoundaryEvenWholeDayDst23Even, GMT_CONTEXT.floorDay(wholeDayDst23Even.getTime(), DAY, 1));
        assertEquals(upperBoundaryEvenWholeDayDst23Even, GMT_CONTEXT.ceilDay(wholeDayDst23Even.getTime() + 1, DAY, 1) -1);
        assertEquals(wholeDayDst23Even,
            new java.sql.Timestamp(GMT_CONTEXT.floorDay(lowerBoundaryEvenWholeDayDst23Even, DAY, 1)));
        assertNotEquals(wholeDayDst23Even,
            new java.sql.Timestamp(GMT_CONTEXT.floorDay(lowerBoundaryEvenWholeDayDst23Even -1, DAY, 1)));
        assertEquals(wholeDayDst23Even,
            new java.sql.Timestamp(GMT_CONTEXT.floorDay(upperBoundaryEvenWholeDayDst23Even, DAY, 1)));
        assertNotEquals(wholeDayDst23Even,
            new java.sql.Timestamp(GMT_CONTEXT.floorDay(upperBoundaryEvenWholeDayDst23Even + 1, DAY, 1)));

        //DST switchover day (6th is day is 25 hours long)
        // No DST in GMT
        java.sql.Timestamp wholeDayDst25Even =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-06 0:0:0").getTime());
        long lowerBoundaryEvenWholeDayDst25Even = wholeDayDst25Even.getTime();
        long upperBoundaryEvenWholeDayDst25Even = wholeDayDst25Even.getTime() + DAY -1;
        assertEquals(lowerBoundaryEvenWholeDayDst25Even, GMT_CONTEXT.floorDay(wholeDayDst25Even.getTime(), DAY, 1));
        assertEquals(upperBoundaryEvenWholeDayDst25Even, GMT_CONTEXT.ceilDay(wholeDayDst25Even.getTime() + 1, DAY, 1) -1);
        assertEquals(wholeDayDst25Even,
            new java.sql.Timestamp(GMT_CONTEXT.floorDay(lowerBoundaryEvenWholeDayDst25Even, DAY, 1)));
        assertNotEquals(wholeDayDst25Even,
            new java.sql.Timestamp(GMT_CONTEXT.floorDay(lowerBoundaryEvenWholeDayDst25Even -1, DAY, 1)));
        assertEquals(wholeDayDst25Even,
            new java.sql.Timestamp(GMT_CONTEXT.floorDay(upperBoundaryEvenWholeDayDst25Even, DAY, 1)));
        assertNotEquals(wholeDayDst25Even,
            new java.sql.Timestamp(GMT_CONTEXT.floorDay(upperBoundaryEvenWholeDayDst25Even + 1, DAY, 1)));

        java.sql.Timestamp wholeWeekOdd =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-10-10 0:0:0").getTime());
        long lowerBoundaryWholeWeekOdd = wholeWeekOdd.getTime();
        long upperBoundaryWholeWeekOdd = wholeWeekOdd.getTime() + WEEK - 1;
        assertEquals(lowerBoundaryWholeWeekOdd, GMT_CONTEXT.floorWeek(wholeWeekOdd.getTime(), WEEK, 1));
        assertEquals(upperBoundaryWholeWeekOdd, GMT_CONTEXT.ceilWeek(wholeWeekOdd.getTime() + 1, WEEK, 1) -1);
        assertEquals(wholeWeekOdd,
            new java.sql.Timestamp(GMT_CONTEXT.floorWeek(lowerBoundaryWholeWeekOdd, WEEK, 1)));
        assertNotEquals(wholeWeekOdd,
            new java.sql.Timestamp(GMT_CONTEXT.floorWeek(lowerBoundaryWholeWeekOdd -1, WEEK, 1)));
        assertEquals(wholeWeekOdd,
            new java.sql.Timestamp(GMT_CONTEXT.floorWeek(upperBoundaryWholeWeekOdd, WEEK, 1)));
        assertNotEquals(wholeWeekOdd,
            new java.sql.Timestamp(GMT_CONTEXT.floorWeek(upperBoundaryWholeWeekOdd + 1, WEEK, 1)));

        // No DST in GMT
        java.sql.Timestamp wholeWeekDst25Even =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-10-31 0:0:0").getTime());
        long lowerBoundaryWholeWeekDst25Even = wholeWeekDst25Even.getTime();
        long upperBoundaryWholeWeekDst25Even = wholeWeekDst25Even.getTime() + WEEK -1;
        assertEquals(lowerBoundaryWholeWeekDst25Even, GMT_CONTEXT.floorWeek(wholeWeekDst25Even.getTime(), WEEK, 1));
        assertEquals(upperBoundaryWholeWeekDst25Even, GMT_CONTEXT.ceilWeek(wholeWeekDst25Even.getTime() +1, WEEK, 1) -1);
        assertEquals(wholeWeekDst25Even,
            new java.sql.Timestamp(GMT_CONTEXT.floorWeek(lowerBoundaryWholeWeekDst25Even, WEEK, 1)));
        assertNotEquals(wholeWeekDst25Even,
            new java.sql.Timestamp(GMT_CONTEXT.floorWeek(lowerBoundaryWholeWeekDst25Even - 1, WEEK, 1)));
        assertEquals(wholeWeekDst25Even,
            new java.sql.Timestamp(GMT_CONTEXT.floorWeek(upperBoundaryWholeWeekDst25Even, WEEK, 1)));
        assertNotEquals(wholeWeekDst25Even,
            new java.sql.Timestamp(GMT_CONTEXT.floorWeek(upperBoundaryWholeWeekDst25Even + 1, WEEK, 1)));

        // Is it even possible for the switchover to be on an Odd week (of Year) ?
        
        // No DST in GMT
        java.sql.Timestamp wholeWeekDst23Even =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-03-07 0:0:0").getTime());
        long lowerBoundaryWholeWeekDst23Even = wholeWeekDst23Even.getTime();
        long upperBoundaryWholeWeekDst23Even = wholeWeekDst23Even.getTime() + WEEK - 1;
        assertEquals(lowerBoundaryWholeWeekDst23Even, GMT_CONTEXT.floorWeek(wholeWeekDst23Even.getTime(), WEEK, 1));
        assertEquals(upperBoundaryWholeWeekDst23Even, GMT_CONTEXT.ceilWeek(wholeWeekDst23Even.getTime() + 1, WEEK, 1) -1);
        assertEquals(wholeWeekDst23Even,
            new java.sql.Timestamp(GMT_CONTEXT.floorWeek(lowerBoundaryWholeWeekDst23Even, WEEK, 1)));
        assertNotEquals(wholeWeekDst23Even,
            new java.sql.Timestamp(GMT_CONTEXT.floorWeek(lowerBoundaryWholeWeekDst23Even -1, WEEK, 1)));
        assertEquals(wholeWeekDst23Even,
            new java.sql.Timestamp(GMT_CONTEXT.floorWeek(upperBoundaryWholeWeekDst23Even, WEEK, 1)));
        assertNotEquals(wholeWeekDst23Even,
            new java.sql.Timestamp(GMT_CONTEXT.floorWeek(upperBoundaryWholeWeekDst23Even + 1, WEEK, 1)));
        
        // Is it even possible for the switchover to be on an Odd week (of Year) ?

        java.sql.Timestamp wholeMonthOdd =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-07-1 0:0:0").getTime());
        long lowerBoundaryWholeMonthOdd = wholeMonthOdd.getTime();
        // July is 31 days
        long upperBoundaryWholeMonthOdd = wholeMonthOdd.getTime() + 31 * DAY - 1;
        assertEquals(lowerBoundaryWholeMonthOdd, GMT_CONTEXT.floorMonth(wholeMonthOdd.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeMonthOdd, GMT_CONTEXT.ceilMonth(wholeMonthOdd.getTime() + 1, 0, 1) -1);
        assertEquals(wholeMonthOdd,
            new java.sql.Timestamp(GMT_CONTEXT.floorMonth(lowerBoundaryWholeMonthOdd, 0, 1)));
        assertNotEquals(wholeMonthOdd,
            new java.sql.Timestamp(GMT_CONTEXT.floorMonth(lowerBoundaryWholeMonthOdd - 1, 0, 1)));
        assertEquals(wholeMonthOdd,
            new java.sql.Timestamp(GMT_CONTEXT.floorMonth(upperBoundaryWholeMonthOdd, 0, 1)));
        assertNotEquals(wholeMonthOdd,
            new java.sql.Timestamp(GMT_CONTEXT.floorMonth(upperBoundaryWholeMonthOdd + 1, 0, 1)));

        // No DST in GMT
        java.sql.Timestamp wholeMonthDst23Odd =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-03-1 0:0:0").getTime());
        long lowerBoundaryWholeMonthDst23Odd = wholeMonthDst23Odd.getTime();
        // March is 31 days
        long upperBoundaryWholeMonthDst23Odd = wholeMonthDst23Odd.getTime() + 31 * DAY - 1;
        assertEquals(lowerBoundaryWholeMonthDst23Odd, GMT_CONTEXT.floorMonth(wholeMonthDst23Odd.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeMonthDst23Odd, GMT_CONTEXT.ceilMonth(wholeMonthDst23Odd.getTime() + 1, 0, 1) -1);
        assertEquals(wholeMonthDst23Odd,
            new java.sql.Timestamp(GMT_CONTEXT.floorMonth(lowerBoundaryWholeMonthDst23Odd, 0, 1)));
        assertNotEquals(wholeMonthDst23Odd,
            new java.sql.Timestamp(GMT_CONTEXT.floorMonth(lowerBoundaryWholeMonthDst23Odd -1, 0, 1)));
        assertEquals(wholeMonthDst23Odd,
            new java.sql.Timestamp(GMT_CONTEXT.floorMonth(upperBoundaryWholeMonthDst23Odd, 0, 1)));
        assertNotEquals(wholeMonthDst23Odd,
            new java.sql.Timestamp(GMT_CONTEXT.floorMonth(upperBoundaryWholeMonthDst23Odd + 1, 0, 1)));

        java.sql.Timestamp wholeMonthLeap =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2024-02-1 0:0:0").getTime());
        long lowerBoundaryWholeMonthLeap = wholeMonthLeap.getTime();
        // February is is 29 days
        long upperBoundaryWholeMonthLeap = wholeMonthLeap.getTime() + 29 * DAY -1;
        assertEquals(lowerBoundaryWholeMonthLeap, GMT_CONTEXT.floorMonth(wholeMonthLeap.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeMonthLeap, GMT_CONTEXT.ceilMonth(wholeMonthLeap.getTime() +1, 0, 1) - 1);
        assertEquals(wholeMonthLeap,
            new java.sql.Timestamp(GMT_CONTEXT.floorMonth(lowerBoundaryWholeMonthLeap, 0, 1)));
        assertNotEquals(wholeMonthLeap,
            new java.sql.Timestamp(GMT_CONTEXT.floorMonth(lowerBoundaryWholeMonthLeap - 1, 0, 1)));
        assertEquals(wholeMonthLeap,
            new java.sql.Timestamp(GMT_CONTEXT.floorMonth(upperBoundaryWholeMonthLeap, 0, 1)));
        assertNotEquals(wholeMonthLeap,
            new java.sql.Timestamp(GMT_CONTEXT.floorMonth(upperBoundaryWholeMonthLeap + 1, 0, 1)));

        java.sql.Timestamp wholeYearEven =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-1-1 0:0:0").getTime());
        long lowerBoundaryWholeYearEven = wholeYearEven.getTime();
        long upperBoundaryWholeYearEven = wholeYearEven.getTime() + YEAR -1;
        assertEquals(lowerBoundaryWholeYearEven, GMT_CONTEXT.floorYear(wholeYearEven.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeYearEven, GMT_CONTEXT.ceilYear(wholeYearEven.getTime() + 1, 0, 1) - 1);
        assertEquals(wholeYearEven,
            new java.sql.Timestamp(GMT_CONTEXT.floorYear(lowerBoundaryWholeYearEven, 0, 1)));
        assertNotEquals(wholeYearEven,
            new java.sql.Timestamp(GMT_CONTEXT.floorYear(lowerBoundaryWholeYearEven -1, 0, 1)));
        assertEquals(wholeYearEven,
            new java.sql.Timestamp(GMT_CONTEXT.floorYear(upperBoundaryWholeYearEven, 0, 1)));
        assertNotEquals(wholeYearEven,
            new java.sql.Timestamp(GMT_CONTEXT.floorYear(upperBoundaryWholeYearEven + 1, 0, 1)));

        java.sql.Timestamp wholeYearLeapEven =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2024-1-1 0:0:0").getTime());
        long lowerBoundaryWholeYearLeapEven = wholeYearLeapEven.getTime();
        long upperBoundaryWholeYearLeapEven = wholeYearLeapEven.getTime() + YEAR + DAY - 1;
        assertEquals(lowerBoundaryWholeYearLeapEven, GMT_CONTEXT.floorYear(wholeYearLeapEven.getTime(), 0, 1));
        assertEquals(upperBoundaryWholeYearLeapEven, GMT_CONTEXT.ceilYear(wholeYearLeapEven.getTime() + 1, 0, 1) -1);
        assertEquals(wholeYearLeapEven,
            new java.sql.Timestamp(GMT_CONTEXT.floorYear(lowerBoundaryWholeYearLeapEven, 0, 1)));
        assertNotEquals(wholeYearLeapEven,
            new java.sql.Timestamp(GMT_CONTEXT.floorYear(lowerBoundaryWholeYearLeapEven - 1, 0, 1)));
        assertEquals(wholeYearLeapEven,
            new java.sql.Timestamp(GMT_CONTEXT.floorYear(upperBoundaryWholeYearLeapEven, 0, 1)));
        assertNotEquals(wholeYearLeapEven,
            new java.sql.Timestamp(GMT_CONTEXT.floorYear(upperBoundaryWholeYearLeapEven + 1, 0, 1)));
    }

    @Test
    public void testCeilCompliant() {
        // We operate on Instants for time units up to Days, simply counting millis
        
        // No need to repeat odd / even cases
        // The logic for upper and lower scan ranges is in the Expression classes. It is always
        // [floor(ts-1)+1, ceil(ts)]
        java.sql.Timestamp oddWholeSecond =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 11:11:11").getTime());
        long lowerBoundaryOddWholeSecond = oddWholeSecond.getTime() - SEC + 1;
        long upperBoundaryOddWholeSecond = oddWholeSecond.getTime();
        assertEquals(lowerBoundaryOddWholeSecond, EST_CONTEXT.floorSecond(oddWholeSecond.getTime() -1, 0, 1) + 1);
        assertEquals(upperBoundaryOddWholeSecond, EST_CONTEXT.ceilSecond(oddWholeSecond.getTime(), 0, 1));
        assertEquals(oddWholeSecond,
            new java.sql.Timestamp(EST_CONTEXT.ceilSecond(lowerBoundaryOddWholeSecond, 0, 1)));
        assertNotEquals(oddWholeSecond,
            new java.sql.Timestamp(EST_CONTEXT.ceilSecond(lowerBoundaryOddWholeSecond -1, 0, 1)));
        assertEquals(oddWholeSecond,
            new java.sql.Timestamp(EST_CONTEXT.ceilSecond(upperBoundaryOddWholeSecond, 0, 1)));
        assertNotEquals(oddWholeSecond,
            new java.sql.Timestamp(EST_CONTEXT.ceilSecond(upperBoundaryOddWholeSecond + 1, 0, 1)));

        // 10 sec range
        java.sql.Timestamp oddWholeSecondCeil10 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 11:11:20").getTime());
        long lowerBoundaryOddWholeSecondCeil10 = oddWholeSecondCeil10.getTime() - 10 * SEC + 1;
        long upperBoundaryOddWholeSecondCeil10 = oddWholeSecondCeil10.getTime();
        assertEquals(lowerBoundaryOddWholeSecondCeil10, EST_CONTEXT.floorSecond(oddWholeSecond.getTime() - 1, 0, 10) + 1);
        assertEquals(upperBoundaryOddWholeSecondCeil10, EST_CONTEXT.ceilSecond(oddWholeSecond.getTime(), 0, 10));
        assertEquals(oddWholeSecondCeil10,
            new java.sql.Timestamp(EST_CONTEXT.ceilSecond(lowerBoundaryOddWholeSecondCeil10, 0, 10)));
        assertNotEquals(oddWholeSecondCeil10,
            new java.sql.Timestamp(EST_CONTEXT.ceilSecond(lowerBoundaryOddWholeSecondCeil10 -1, 0, 10)));
        assertEquals(oddWholeSecondCeil10,
            new java.sql.Timestamp(EST_CONTEXT.ceilSecond(upperBoundaryOddWholeSecondCeil10, 0, 10)));
        assertNotEquals(oddWholeSecondCeil10,
            new java.sql.Timestamp(EST_CONTEXT.ceilSecond(upperBoundaryOddWholeSecondCeil10 + 1, 0, 10)));
        
        // 15 sec range
        java.sql.Timestamp oddWholeSecondCeil15 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 11:11:15").getTime());
        long lowerBoundaryOddWholeSecondFloor15 = oddWholeSecondCeil15.getTime() - 15 * SEC+ 1;
        long upperBoundaryOddWholeSecondFloor15 = oddWholeSecondCeil15.getTime();
        assertEquals(lowerBoundaryOddWholeSecondFloor15, EST_CONTEXT.floorSecond(oddWholeSecond.getTime() - 1, 0, 15) + 1);
        assertEquals(upperBoundaryOddWholeSecondFloor15, EST_CONTEXT.ceilSecond(oddWholeSecond.getTime(), 0, 15));
        assertEquals(oddWholeSecondCeil15,
            new java.sql.Timestamp(EST_CONTEXT.ceilSecond(lowerBoundaryOddWholeSecondFloor15, 0, 15)));
        assertNotEquals(oddWholeSecondCeil15,
            new java.sql.Timestamp(EST_CONTEXT.ceilSecond(lowerBoundaryOddWholeSecondFloor15 -1, 0, 15)));
        assertEquals(oddWholeSecondCeil15,
            new java.sql.Timestamp(EST_CONTEXT.ceilSecond(upperBoundaryOddWholeSecondFloor15, 0, 15)));
        assertNotEquals(oddWholeSecondCeil15,
            new java.sql.Timestamp(EST_CONTEXT.ceilSecond(upperBoundaryOddWholeSecondFloor15 + 1, 0, 15)));
        
        java.sql.Timestamp evenWholeMinute =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 11:12:0").getTime());
        long lowerBoundaryEvenWholeMinute = evenWholeMinute.getTime() - MIN + 1;
        long upperBoundaryEvenWholeMinute = evenWholeMinute.getTime();
        assertEquals(lowerBoundaryEvenWholeMinute, EST_CONTEXT.floorMinute(evenWholeMinute.getTime() -1, 0, 1) + 1);
        assertEquals(upperBoundaryEvenWholeMinute, EST_CONTEXT.ceilMinute(evenWholeMinute.getTime(), 0, 1));
        assertEquals(evenWholeMinute,
            new java.sql.Timestamp(EST_CONTEXT.ceilMinute(lowerBoundaryEvenWholeMinute, 0, 1)));
        assertNotEquals(evenWholeMinute,
            new java.sql.Timestamp(EST_CONTEXT.ceilMinute(lowerBoundaryEvenWholeMinute - 1, 0, 1)));
        assertEquals(evenWholeMinute,
            new java.sql.Timestamp(EST_CONTEXT.ceilMinute(upperBoundaryEvenWholeMinute, 0, 1)));
        assertNotEquals(evenWholeMinute,
            new java.sql.Timestamp(EST_CONTEXT.ceilMinute(upperBoundaryEvenWholeMinute + 1, 0, 1)));

        java.sql.Timestamp evenWholeMinuteCeil20 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 11:20:0").getTime());
        long lowerBoundaryEvenWholeMinuteCeil20 = evenWholeMinuteCeil20.getTime() - 20 * MIN + 1;
        long upperBoundaryEvenWholeMinuteCeil20 = evenWholeMinuteCeil20.getTime();
        assertEquals(lowerBoundaryEvenWholeMinuteCeil20, EST_CONTEXT.floorMinute(evenWholeMinute.getTime() -1 , 0, 20) + 1);
        assertEquals(upperBoundaryEvenWholeMinuteCeil20, EST_CONTEXT.ceilMinute(evenWholeMinute.getTime(), 0, 20));
        assertEquals(evenWholeMinuteCeil20,
            new java.sql.Timestamp(EST_CONTEXT.ceilMinute(lowerBoundaryEvenWholeMinuteCeil20, 0, 20)));
        assertNotEquals(evenWholeMinuteCeil20,
            new java.sql.Timestamp(EST_CONTEXT.ceilMinute(lowerBoundaryEvenWholeMinuteCeil20 -1, 0, 20)));
        assertEquals(evenWholeMinuteCeil20,
            new java.sql.Timestamp(EST_CONTEXT.ceilMinute(upperBoundaryEvenWholeMinuteCeil20, 0, 20)));
        assertNotEquals(evenWholeMinuteCeil20,
            new java.sql.Timestamp(EST_CONTEXT.ceilMinute(upperBoundaryEvenWholeMinuteCeil20 + 1, 0, 20)));
        
        //Minutes since epoch, don't expect the rounded value to be "round"
        java.sql.Timestamp evenWholeMinuteCeil17 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 11:12:00").getTime());
        long lowerBoundaryEvenWholeMinute17 = evenWholeMinuteCeil17.getTime() - 17 * MIN + 1;
        long upperBoundaryEvenWholeMinute17 = evenWholeMinuteCeil17.getTime();
        assertEquals(lowerBoundaryEvenWholeMinute17, EST_CONTEXT.floorMinute(evenWholeMinute.getTime() - 1, 0, 17) + 1);
        assertEquals(upperBoundaryEvenWholeMinute17, EST_CONTEXT.ceilMinute(evenWholeMinute.getTime(), 0, 17));
        assertEquals(evenWholeMinuteCeil17,
            new java.sql.Timestamp(EST_CONTEXT.ceilMinute(lowerBoundaryEvenWholeMinute17, 0, 17)));
        assertNotEquals(evenWholeMinuteCeil17,
            new java.sql.Timestamp(EST_CONTEXT.ceilMinute(lowerBoundaryEvenWholeMinute17 -1, 0, 17)));
        assertEquals(evenWholeMinuteCeil17,
            new java.sql.Timestamp(EST_CONTEXT.ceilMinute(upperBoundaryEvenWholeMinute17, 0, 17)));
        assertNotEquals(evenWholeMinuteCeil17,
            new java.sql.Timestamp(EST_CONTEXT.ceilMinute(upperBoundaryEvenWholeMinute17 + 1, 0, 17)));

        java.sql.Timestamp oddWholeHour =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 11:0:0").getTime());
        long lowerBoundaryOddWholeHour = oddWholeHour.getTime() - HOUR + 1;
        long upperBoundaryOddWholeHour = oddWholeHour.getTime();
        assertEquals(lowerBoundaryOddWholeHour, EST_CONTEXT.floorHour(oddWholeHour.getTime() -1, 0, 1) + 1);
        assertEquals(upperBoundaryOddWholeHour, EST_CONTEXT.ceilHour(oddWholeHour.getTime(), 0, 1));
        assertEquals(oddWholeHour,
            new java.sql.Timestamp(EST_CONTEXT.ceilHour(lowerBoundaryOddWholeHour, 0, 1)));
        assertNotEquals(oddWholeHour,
            new java.sql.Timestamp(EST_CONTEXT.ceilHour(lowerBoundaryOddWholeHour - 1, 0, 1)));
        assertEquals(oddWholeHour,
            new java.sql.Timestamp(EST_CONTEXT.ceilHour(upperBoundaryOddWholeHour, 0, 1)));
        assertNotEquals(oddWholeHour,
            new java.sql.Timestamp(EST_CONTEXT.ceilHour(upperBoundaryOddWholeHour + 1, 0, 1)));

        //Not rounding to hourOfDay
        java.sql.Timestamp oddWholeHour10 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 12:0:0").getTime());
        long lowerBoundaryOddWholeHour10 = oddWholeHour10.getTime() - 10 * HOUR + 1;
        long upperBoundaryOddWholeHour10 = oddWholeHour10.getTime();
        assertEquals(lowerBoundaryOddWholeHour10, EST_CONTEXT.floorHour(oddWholeHour.getTime() -1 , 0, 10) + 1);
        assertEquals(upperBoundaryOddWholeHour10, EST_CONTEXT.ceilHour(oddWholeHour.getTime(), 0, 10));
        assertEquals(oddWholeHour10,
            new java.sql.Timestamp(EST_CONTEXT.ceilHour(lowerBoundaryOddWholeHour10, 0, 10)));
        assertNotEquals(oddWholeHour10,
            new java.sql.Timestamp(EST_CONTEXT.ceilHour(lowerBoundaryOddWholeHour10 - 1, 0, 10)));
        assertEquals(oddWholeHour10,
            new java.sql.Timestamp(EST_CONTEXT.ceilHour(upperBoundaryOddWholeHour10, 0, 10)));
        assertNotEquals(oddWholeHour10,
            new java.sql.Timestamp(EST_CONTEXT.ceilHour(upperBoundaryOddWholeHour10 + 1, 0, 10)));
        
        //Not rounding to hourOfDay
        java.sql.Timestamp oddWholeHour11 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-11 18:0:0").getTime());
        long lowerBoundaryOddWholeHour11 = oddWholeHour11.getTime() - 11 * HOUR + 1;
        long upperBoundaryOddWholeHour11 = oddWholeHour11.getTime();
        assertEquals(lowerBoundaryOddWholeHour11, EST_CONTEXT.floorHour(oddWholeHour.getTime() -1, 0, 11) + 1);
        assertEquals(upperBoundaryOddWholeHour11, EST_CONTEXT.ceilHour(oddWholeHour.getTime(), 0, 11));
        assertEquals(oddWholeHour11,
            new java.sql.Timestamp(EST_CONTEXT.ceilHour(lowerBoundaryOddWholeHour11, 0, 11)));
        assertNotEquals(oddWholeHour11,
            new java.sql.Timestamp(EST_CONTEXT.ceilHour(lowerBoundaryOddWholeHour11 - 1, 0, 11)));
        assertEquals(oddWholeHour11,
            new java.sql.Timestamp(EST_CONTEXT.ceilHour(upperBoundaryOddWholeHour11, 0, 11)));
        assertNotEquals(oddWholeHour11,
            new java.sql.Timestamp(EST_CONTEXT.ceilHour(upperBoundaryOddWholeHour11 + 1, 0, 11)));

        
        // From Days and larger periods, we're using proper date arithmetics 
        // No DST switchover
        java.sql.Timestamp evenWholeDay =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-12 0:0:0").getTime());
        long lowerBoundaryEvenWholeDay = evenWholeDay.getTime() - DAY + 1;
        long upperBoundaryEvenWholeDay = evenWholeDay.getTime();
        assertEquals(lowerBoundaryEvenWholeDay, EST_CONTEXT.floorDay(evenWholeDay.getTime() - 1, 0, 1) + 1);
        assertEquals(upperBoundaryEvenWholeDay, EST_CONTEXT.ceilDay(evenWholeDay.getTime(), 0, 1));
        assertEquals(evenWholeDay,
            new java.sql.Timestamp(EST_CONTEXT.ceilDay(lowerBoundaryEvenWholeDay, 0, 1)));
        assertNotEquals(evenWholeDay,
            new java.sql.Timestamp(EST_CONTEXT.ceilDay(lowerBoundaryEvenWholeDay -1, 0, 1)));
        assertEquals(evenWholeDay,
            new java.sql.Timestamp(EST_CONTEXT.ceilDay(upperBoundaryEvenWholeDay, 0, 1)));
        assertNotEquals(evenWholeDay,
            new java.sql.Timestamp(EST_CONTEXT.ceilDay(upperBoundaryEvenWholeDay + 1, 0, 1)));

        java.sql.Timestamp evenWholeDay2 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-12 0:0:0").getTime());
        long lowerBoundaryEvenWholeDay2 = evenWholeDay2.getTime() - 2 * DAY + 1;
        long upperBoundaryEvenWholeDay2 = evenWholeDay2.getTime();
        assertEquals(lowerBoundaryEvenWholeDay2, EST_CONTEXT.floorDay(evenWholeDay.getTime() - 1, 0, 2) + 1);
        assertEquals(upperBoundaryEvenWholeDay2, EST_CONTEXT.ceilDay(evenWholeDay.getTime(), 0, 2));
        assertEquals(evenWholeDay2,
            new java.sql.Timestamp(EST_CONTEXT.ceilDay(lowerBoundaryEvenWholeDay2, 0, 2)));
        assertNotEquals(evenWholeDay2,
            new java.sql.Timestamp(EST_CONTEXT.ceilDay(lowerBoundaryEvenWholeDay2 - 1, 0, 2)));
        assertEquals(evenWholeDay2,
            new java.sql.Timestamp(EST_CONTEXT.ceilDay(upperBoundaryEvenWholeDay2, 0, 2)));
        assertNotEquals(evenWholeDay2,
            new java.sql.Timestamp(EST_CONTEXT.ceilDay(upperBoundaryEvenWholeDay2 + 1, 0, 2)));

        java.sql.Timestamp evenWholeDay3 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-12 0:0:0").getTime());
        long lowerBoundaryEvenWholeDay3 = evenWholeDay3.getTime() - 3 * DAY + 1;
        long upperBoundaryEvenWholeDay3 = evenWholeDay3.getTime();
        assertEquals(lowerBoundaryEvenWholeDay3, EST_CONTEXT.floorDay(evenWholeDay.getTime() -1, 0, 3) + 1);
        assertEquals(upperBoundaryEvenWholeDay3, EST_CONTEXT.ceilDay(evenWholeDay.getTime(), 0, 3));
        assertEquals(evenWholeDay3,
            new java.sql.Timestamp(EST_CONTEXT.ceilDay(lowerBoundaryEvenWholeDay3, 0, 3)));
        assertNotEquals(evenWholeDay3,
            new java.sql.Timestamp(EST_CONTEXT.ceilDay(lowerBoundaryEvenWholeDay3 - 1, 0, 3)));
        assertEquals(evenWholeDay3,
            new java.sql.Timestamp(EST_CONTEXT.ceilDay(upperBoundaryEvenWholeDay3, 0, 3)));
        assertNotEquals(evenWholeDay3,
            new java.sql.Timestamp(EST_CONTEXT.ceilDay(upperBoundaryEvenWholeDay3 + 1, 0, 3)));
        
        //DST switchover day (12th is 23 hours long)
        java.sql.Timestamp wholeDayDst23 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2023-03-13 0:0:0").getTime());
        long lowerBoundaryEvenWholeDayDst23Even = wholeDayDst23.getTime() - (DAY - HOUR) + 1;
        long upperBoundaryEvenWholeDayDst23Even = wholeDayDst23.getTime();
        assertEquals(lowerBoundaryEvenWholeDayDst23Even, EST_CONTEXT.floorDay(wholeDayDst23.getTime() -1, 0, 1) + 1);
        assertEquals(upperBoundaryEvenWholeDayDst23Even, EST_CONTEXT.ceilDay(wholeDayDst23.getTime(), 0, 1));
        assertEquals(wholeDayDst23,
            new java.sql.Timestamp(EST_CONTEXT.ceilDay(lowerBoundaryEvenWholeDayDst23Even, 0, 1)));
        assertNotEquals(wholeDayDst23,
            new java.sql.Timestamp(EST_CONTEXT.ceilDay(lowerBoundaryEvenWholeDayDst23Even -1, 0, 1)));
        assertEquals(wholeDayDst23,
            new java.sql.Timestamp(EST_CONTEXT.ceilDay(upperBoundaryEvenWholeDayDst23Even, 0, 1)));
        assertNotEquals(wholeDayDst23,
            new java.sql.Timestamp(EST_CONTEXT.ceilDay(upperBoundaryEvenWholeDayDst23Even + 1, 0, 1)));

        java.sql.Timestamp wholeDayDst23Odd =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-03-13 0:0:0").getTime());
        java.sql.Timestamp wholeDayDst23Odd10 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-03-19 0:0:0").getTime());
        long lowerBoundaryEvenWholeDayDst23Odd10 = wholeDayDst23Odd10.getTime() - (10 * DAY - HOUR) + 1;
        long upperBoundaryEvenWholeDayDst23Odd10 = wholeDayDst23Odd10.getTime();
        assertEquals(lowerBoundaryEvenWholeDayDst23Odd10, EST_CONTEXT.floorDay(wholeDayDst23Odd.getTime() -1 , 0, 10) + 1);
        assertEquals(upperBoundaryEvenWholeDayDst23Odd10, EST_CONTEXT.ceilDay(wholeDayDst23Odd.getTime(), 0, 10));
        assertEquals(wholeDayDst23Odd10,
            new java.sql.Timestamp(EST_CONTEXT.ceilDay(lowerBoundaryEvenWholeDayDst23Odd10, 0, 10)));
        assertNotEquals(wholeDayDst23Odd10,
            new java.sql.Timestamp(EST_CONTEXT.ceilDay(lowerBoundaryEvenWholeDayDst23Odd10 - 1, 0, 10)));
        assertEquals(wholeDayDst23Odd10,
            new java.sql.Timestamp(EST_CONTEXT.ceilDay(upperBoundaryEvenWholeDayDst23Odd10, 0, 10)));
        assertNotEquals(wholeDayDst23Odd10,
            new java.sql.Timestamp(EST_CONTEXT.ceilDay(upperBoundaryEvenWholeDayDst23Odd10 + 1, 0, 10)));
        
        java.sql.Timestamp wholeDayDst23Odd27 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-04-07 0:0:0").getTime());
        long lowerBoundaryEvenWholeDayDst23Odd27 = wholeDayDst23Odd27.getTime() - (27 * DAY - HOUR) + 1;
        long upperBoundaryEvenWholeDayDst23Odd27 = wholeDayDst23Odd27.getTime();
        assertEquals(lowerBoundaryEvenWholeDayDst23Odd27, EST_CONTEXT.floorDay(wholeDayDst23Odd.getTime() - 1, 0, 27) + 1 );
        assertEquals(upperBoundaryEvenWholeDayDst23Odd27, EST_CONTEXT.ceilDay(wholeDayDst23Odd.getTime(), 0, 27));
        assertEquals(wholeDayDst23Odd27,
            new java.sql.Timestamp(EST_CONTEXT.ceilDay(lowerBoundaryEvenWholeDayDst23Odd27, 0, 27)));
        assertNotEquals(wholeDayDst23Odd27,
            new java.sql.Timestamp(EST_CONTEXT.ceilDay(lowerBoundaryEvenWholeDayDst23Odd27 - 1, 0, 27)));
        assertEquals(wholeDayDst23Odd27,
            new java.sql.Timestamp(EST_CONTEXT.ceilDay(upperBoundaryEvenWholeDayDst23Odd27, 0, 27)));
        assertNotEquals(wholeDayDst23Odd27,
            new java.sql.Timestamp(EST_CONTEXT.ceilDay(upperBoundaryEvenWholeDayDst23Odd27 + 1, 0, 27)));

        
        //DST switchover day (6th is day is 25 hours long)
        java.sql.Timestamp wholeDayDst25Even =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-07 0:0:0").getTime());
        long lowerBoundaryEvenWholeDayDst25Even = wholeDayDst25Even.getTime() - (DAY + HOUR) + 1;
        long upperBoundaryEvenWholeDayDst25Even = wholeDayDst25Even.getTime();
        assertEquals(lowerBoundaryEvenWholeDayDst25Even, EST_CONTEXT.floorDay(wholeDayDst25Even.getTime() -1, 0, 1) + 1);
        assertEquals(upperBoundaryEvenWholeDayDst25Even, EST_CONTEXT.ceilDay(wholeDayDst25Even.getTime(), 0, 1));
        assertEquals(wholeDayDst25Even,
            new java.sql.Timestamp(EST_CONTEXT.ceilDay(lowerBoundaryEvenWholeDayDst25Even, 0, 1)));
        assertNotEquals(wholeDayDst25Even,
            new java.sql.Timestamp(EST_CONTEXT.ceilDay(lowerBoundaryEvenWholeDayDst25Even -1, 0, 1)));
        assertEquals(wholeDayDst25Even,
            new java.sql.Timestamp(EST_CONTEXT.ceilDay(upperBoundaryEvenWholeDayDst25Even, 0, 1)));
        assertNotEquals(wholeDayDst25Even,
            new java.sql.Timestamp(EST_CONTEXT.ceilDay(upperBoundaryEvenWholeDayDst25Even + 1, 0, 1)));

        java.sql.Timestamp wholeWeekOdd =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-10-10 0:0:0").getTime());
        long lowerBoundaryWholeWeekOdd = wholeWeekOdd.getTime() - WEEK + 1;
        long upperBoundaryWholeWeekOdd = wholeWeekOdd.getTime();
        assertEquals(lowerBoundaryWholeWeekOdd, EST_CONTEXT.floorWeek(wholeWeekOdd.getTime() -1, 0, 1) +1);
        assertEquals(upperBoundaryWholeWeekOdd, EST_CONTEXT.ceilWeek(wholeWeekOdd.getTime(), 0, 1));
        assertEquals(wholeWeekOdd,
            new java.sql.Timestamp(EST_CONTEXT.ceilWeek(lowerBoundaryWholeWeekOdd, 0, 1)));
        assertNotEquals(wholeWeekOdd,
            new java.sql.Timestamp(EST_CONTEXT.ceilWeek(lowerBoundaryWholeWeekOdd -1, 0, 1)));
        assertEquals(wholeWeekOdd,
            new java.sql.Timestamp(EST_CONTEXT.ceilWeek(upperBoundaryWholeWeekOdd, 0, 1)));
        assertNotEquals(wholeWeekOdd,
            new java.sql.Timestamp(EST_CONTEXT.ceilWeek(upperBoundaryWholeWeekOdd + 1, 0, 1)));

        java.sql.Timestamp wholeWeekDst25 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-11-07 0:0:0").getTime());
        long lowerBoundaryWholeWeekDst25Even = wholeWeekDst25.getTime() - (WEEK + HOUR) + 1;
        long upperBoundaryWholeWeekDst25Even = wholeWeekDst25.getTime();
        assertEquals(lowerBoundaryWholeWeekDst25Even, EST_CONTEXT.floorWeek(wholeWeekDst25.getTime() - 1, 0, 1) + 1);
        assertEquals(upperBoundaryWholeWeekDst25Even, EST_CONTEXT.ceilWeek(wholeWeekDst25.getTime(), 0, 1));
        assertEquals(wholeWeekDst25,
            new java.sql.Timestamp(EST_CONTEXT.ceilWeek(lowerBoundaryWholeWeekDst25Even, 0, 1)));
        assertNotEquals(wholeWeekDst25,
            new java.sql.Timestamp(EST_CONTEXT.ceilWeek(lowerBoundaryWholeWeekDst25Even - 1, 0, 1)));
        assertEquals(wholeWeekDst25,
            new java.sql.Timestamp(EST_CONTEXT.ceilWeek(upperBoundaryWholeWeekDst25Even, 0, 1)));
        assertNotEquals(wholeWeekDst25,
            new java.sql.Timestamp(EST_CONTEXT.ceilWeek(upperBoundaryWholeWeekDst25Even + 1, 0, 1)));

        // Is it even possible for the switchover to be on an Odd week (of Year) ?
        
        java.sql.Timestamp wholeWeekDst23 =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-03-14 0:0:0").getTime());
        long lowerBoundaryWholeWeekDst23Even = wholeWeekDst23.getTime() - (WEEK - HOUR) + 1;
        long upperBoundaryWholeWeekDst23Even = wholeWeekDst23.getTime();
        assertEquals(lowerBoundaryWholeWeekDst23Even, EST_CONTEXT.floorWeek(wholeWeekDst23.getTime() - 1, 0, 1) + 1);
        assertEquals(upperBoundaryWholeWeekDst23Even, EST_CONTEXT.ceilWeek(wholeWeekDst23.getTime(), 0, 1));
        assertEquals(wholeWeekDst23,
            new java.sql.Timestamp(EST_CONTEXT.ceilWeek(lowerBoundaryWholeWeekDst23Even, 0, 1)));
        assertNotEquals(wholeWeekDst23,
            new java.sql.Timestamp(EST_CONTEXT.ceilWeek(lowerBoundaryWholeWeekDst23Even -1, 0, 1)));
        assertEquals(wholeWeekDst23,
            new java.sql.Timestamp(EST_CONTEXT.ceilWeek(upperBoundaryWholeWeekDst23Even, 0, 1)));
        assertNotEquals(wholeWeekDst23,
            new java.sql.Timestamp(EST_CONTEXT.ceilWeek(upperBoundaryWholeWeekDst23Even + 1, 0, 1)));
        
        // Is it even possible for the switchover to be on an Odd week (of Year) ?

        java.sql.Timestamp wholeMonthOdd =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-08-1 0:0:0").getTime());
        // July is 31 days
        long lowerBoundaryWholeMonthOdd = wholeMonthOdd.getTime() - 31 * DAY + 1;
        long upperBoundaryWholeMonthOdd = wholeMonthOdd.getTime();
        assertEquals(lowerBoundaryWholeMonthOdd, EST_CONTEXT.floorMonth(wholeMonthOdd.getTime() -1, 0, 1) + 1);
        assertEquals(upperBoundaryWholeMonthOdd, EST_CONTEXT.ceilMonth(wholeMonthOdd.getTime(), 0, 1));
        assertEquals(wholeMonthOdd,
            new java.sql.Timestamp(EST_CONTEXT.ceilMonth(lowerBoundaryWholeMonthOdd, 0, 1)));
        assertNotEquals(wholeMonthOdd,
            new java.sql.Timestamp(EST_CONTEXT.ceilMonth(lowerBoundaryWholeMonthOdd - 1, 0, 1)));
        assertEquals(wholeMonthOdd,
            new java.sql.Timestamp(EST_CONTEXT.ceilMonth(upperBoundaryWholeMonthOdd, 0, 1)));
        assertNotEquals(wholeMonthOdd,
            new java.sql.Timestamp(EST_CONTEXT.ceilMonth(upperBoundaryWholeMonthOdd + 1, 0, 1)));

        java.sql.Timestamp wholeMonthDst23Odd =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-04-1 0:0:0").getTime());
        // March is 31 days
        long lowerBoundaryWholeMonthDst23 = wholeMonthDst23Odd.getTime() - 31 * DAY + HOUR + 1;
        long upperBoundaryWholeMonthDst23Odd = wholeMonthDst23Odd.getTime();
        assertEquals(lowerBoundaryWholeMonthDst23, EST_CONTEXT.floorMonth(wholeMonthDst23Odd.getTime() -1, 0, 1) + 1);
        assertEquals(upperBoundaryWholeMonthDst23Odd, EST_CONTEXT.ceilMonth(wholeMonthDst23Odd.getTime(), 0, 1));
        assertEquals(wholeMonthDst23Odd,
            new java.sql.Timestamp(EST_CONTEXT.ceilMonth(lowerBoundaryWholeMonthDst23, 0, 1)));
        assertNotEquals(wholeMonthDst23Odd,
            new java.sql.Timestamp(EST_CONTEXT.ceilMonth(lowerBoundaryWholeMonthDst23 -1, 0, 1)));
        assertEquals(wholeMonthDst23Odd,
            new java.sql.Timestamp(EST_CONTEXT.ceilMonth(upperBoundaryWholeMonthDst23Odd, 0, 1)));
        assertNotEquals(wholeMonthDst23Odd,
            new java.sql.Timestamp(EST_CONTEXT.ceilMonth(upperBoundaryWholeMonthDst23Odd + 1, 0, 1)));

        java.sql.Timestamp wholeMonthLeap =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2024-03-1 0:0:0").getTime());
        // February is is 29 days
        long lowerBoundaryWholeMonthLeap = wholeMonthLeap.getTime() - 29 * DAY + 1;
        long upperBoundaryWholeMonthLeap = wholeMonthLeap.getTime();
        assertEquals(lowerBoundaryWholeMonthLeap, EST_CONTEXT.floorMonth(wholeMonthLeap.getTime() -1, 0, 1) + 1);
        assertEquals(upperBoundaryWholeMonthLeap, EST_CONTEXT.ceilMonth(wholeMonthLeap.getTime(), 0, 1));
        assertEquals(wholeMonthLeap,
            new java.sql.Timestamp(EST_CONTEXT.ceilMonth(lowerBoundaryWholeMonthLeap, 0, 1)));
        assertNotEquals(wholeMonthLeap,
            new java.sql.Timestamp(EST_CONTEXT.ceilMonth(lowerBoundaryWholeMonthLeap - 1, 0, 1)));
        assertEquals(wholeMonthLeap,
            new java.sql.Timestamp(EST_CONTEXT.ceilMonth(upperBoundaryWholeMonthLeap, 0, 1)));
        assertNotEquals(wholeMonthLeap,
            new java.sql.Timestamp(EST_CONTEXT.ceilMonth(upperBoundaryWholeMonthLeap + 1, 0, 1)));

        java.sql.Timestamp wholeYearEven =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2022-1-1 0:0:0").getTime());
        long lowerBoundaryWholeYearEven = wholeYearEven.getTime() - YEAR + 1;
        long upperBoundaryWholeYearEven = wholeYearEven.getTime();
        assertEquals(lowerBoundaryWholeYearEven, EST_CONTEXT.floorYear(wholeYearEven.getTime() -1, 0, 1) + 1);
        assertEquals(upperBoundaryWholeYearEven, EST_CONTEXT.ceilYear(wholeYearEven.getTime(), 0, 1));
        assertEquals(wholeYearEven,
            new java.sql.Timestamp(EST_CONTEXT.ceilYear(lowerBoundaryWholeYearEven, 0, 1)));
        assertNotEquals(wholeYearEven,
            new java.sql.Timestamp(EST_CONTEXT.ceilYear(lowerBoundaryWholeYearEven -1, 0, 1)));
        assertEquals(wholeYearEven,
            new java.sql.Timestamp(EST_CONTEXT.ceilYear(upperBoundaryWholeYearEven, 0, 1)));
        assertNotEquals(wholeYearEven,
            new java.sql.Timestamp(EST_CONTEXT.ceilYear(upperBoundaryWholeYearEven + 1, 0, 1)));

        java.sql.Timestamp wholeYearLeapEven =
                new java.sql.Timestamp(EST_CONTEXT.parseDate("2025-1-1 0:0:0").getTime());
        long lowerBoundaryWholeYearLeapEven = wholeYearLeapEven.getTime() - (YEAR + DAY) + 1;
        long upperBoundaryWholeYearLeapEven = wholeYearLeapEven.getTime();
        assertEquals(lowerBoundaryWholeYearLeapEven, EST_CONTEXT.floorYear(wholeYearLeapEven.getTime() -1, 0, 1) + 1);
        assertEquals(upperBoundaryWholeYearLeapEven, EST_CONTEXT.ceilYear(wholeYearLeapEven.getTime(), 0, 1));
        assertEquals(wholeYearLeapEven,
            new java.sql.Timestamp(EST_CONTEXT.ceilYear(lowerBoundaryWholeYearLeapEven, 0, 1)));
        assertNotEquals(wholeYearLeapEven,
            new java.sql.Timestamp(EST_CONTEXT.ceilYear(lowerBoundaryWholeYearLeapEven - 1, 0, 1)));
        assertEquals(wholeYearLeapEven,
            new java.sql.Timestamp(EST_CONTEXT.ceilYear(upperBoundaryWholeYearLeapEven, 0, 1)));
        assertNotEquals(wholeYearLeapEven,
            new java.sql.Timestamp(EST_CONTEXT.ceilYear(upperBoundaryWholeYearLeapEven + 1, 0, 1)));
    }

    @Test
    public void testCeilGMT() {
        // We operate on Instants for time units up to Days, simply counting millis
        
        // No need to repeat odd / even cases
        // The logic for upper and lower scan ranges is in the Expression classes. It is always
        // [floor(ts-1)+1, ceil(ts)]
        java.sql.Timestamp oddWholeSecond =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 11:11:11").getTime());
        long lowerBoundaryOddWholeSecond = oddWholeSecond.getTime() - SEC + 1;
        long upperBoundaryOddWholeSecond = oddWholeSecond.getTime();
        assertEquals(lowerBoundaryOddWholeSecond, GMT_CONTEXT.floorSecond(oddWholeSecond.getTime() -1, SEC, 1) + 1);
        assertEquals(upperBoundaryOddWholeSecond, GMT_CONTEXT.ceilSecond(oddWholeSecond.getTime(), SEC, 1));
        assertEquals(oddWholeSecond,
            new java.sql.Timestamp(GMT_CONTEXT.ceilSecond(lowerBoundaryOddWholeSecond, SEC, 1)));
        assertNotEquals(oddWholeSecond,
            new java.sql.Timestamp(GMT_CONTEXT.ceilSecond(lowerBoundaryOddWholeSecond -1, SEC, 1)));
        assertEquals(oddWholeSecond,
            new java.sql.Timestamp(GMT_CONTEXT.ceilSecond(upperBoundaryOddWholeSecond, SEC, 1)));
        assertNotEquals(oddWholeSecond,
            new java.sql.Timestamp(GMT_CONTEXT.ceilSecond(upperBoundaryOddWholeSecond + 1, SEC, 1)));

        // 10 sec range
        java.sql.Timestamp oddWholeSecondCeil10 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 11:11:20").getTime());
        long lowerBoundaryOddWholeSecondCeil10 = oddWholeSecondCeil10.getTime() - 10 * SEC + 1;
        long upperBoundaryOddWholeSecondCeil10 = oddWholeSecondCeil10.getTime();
        assertEquals(lowerBoundaryOddWholeSecondCeil10, GMT_CONTEXT.floorSecond(oddWholeSecond.getTime() - 1, 10 * SEC, 10) + 1);
        assertEquals(upperBoundaryOddWholeSecondCeil10, GMT_CONTEXT.ceilSecond(oddWholeSecond.getTime(), 10 * SEC, 10));
        assertEquals(oddWholeSecondCeil10,
            new java.sql.Timestamp(GMT_CONTEXT.ceilSecond(lowerBoundaryOddWholeSecondCeil10, 10 * SEC, 10)));
        assertNotEquals(oddWholeSecondCeil10,
            new java.sql.Timestamp(GMT_CONTEXT.ceilSecond(lowerBoundaryOddWholeSecondCeil10 -1, 10 * SEC, 10)));
        assertEquals(oddWholeSecondCeil10,
            new java.sql.Timestamp(GMT_CONTEXT.ceilSecond(upperBoundaryOddWholeSecondCeil10, 10 * SEC, 10)));
        assertNotEquals(oddWholeSecondCeil10,
            new java.sql.Timestamp(GMT_CONTEXT.ceilSecond(upperBoundaryOddWholeSecondCeil10 + 1, 10 * SEC, 10)));
        
        // 15 sec range
        java.sql.Timestamp oddWholeSecondCeil15 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 11:11:15").getTime());
        long lowerBoundaryOddWholeSecondFloor15 = oddWholeSecondCeil15.getTime() - 15 * SEC+ 1;
        long upperBoundaryOddWholeSecondFloor15 = oddWholeSecondCeil15.getTime();
        assertEquals(lowerBoundaryOddWholeSecondFloor15, GMT_CONTEXT.floorSecond(oddWholeSecond.getTime() - 1, 15 * SEC, 15) + 1);
        assertEquals(upperBoundaryOddWholeSecondFloor15, GMT_CONTEXT.ceilSecond(oddWholeSecond.getTime(), 15 * SEC, 15));
        assertEquals(oddWholeSecondCeil15,
            new java.sql.Timestamp(GMT_CONTEXT.ceilSecond(lowerBoundaryOddWholeSecondFloor15, 15 * SEC, 15)));
        assertNotEquals(oddWholeSecondCeil15,
            new java.sql.Timestamp(GMT_CONTEXT.ceilSecond(lowerBoundaryOddWholeSecondFloor15 -1, 15 * SEC, 15)));
        assertEquals(oddWholeSecondCeil15,
            new java.sql.Timestamp(GMT_CONTEXT.ceilSecond(upperBoundaryOddWholeSecondFloor15, 15 * SEC, 15)));
        assertNotEquals(oddWholeSecondCeil15,
            new java.sql.Timestamp(GMT_CONTEXT.ceilSecond(upperBoundaryOddWholeSecondFloor15 + 1, 15 * SEC, 15)));
        
        java.sql.Timestamp evenWholeMinute =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 11:12:0").getTime());
        long lowerBoundaryEvenWholeMinute = evenWholeMinute.getTime() - MIN + 1;
        long upperBoundaryEvenWholeMinute = evenWholeMinute.getTime();
        assertEquals(lowerBoundaryEvenWholeMinute, GMT_CONTEXT.floorMinute(evenWholeMinute.getTime() -1, MIN, 1) + 1);
        assertEquals(upperBoundaryEvenWholeMinute, GMT_CONTEXT.ceilMinute(evenWholeMinute.getTime(), MIN, 1));
        assertEquals(evenWholeMinute,
            new java.sql.Timestamp(GMT_CONTEXT.ceilMinute(lowerBoundaryEvenWholeMinute, MIN, 1)));
        assertNotEquals(evenWholeMinute,
            new java.sql.Timestamp(GMT_CONTEXT.ceilMinute(lowerBoundaryEvenWholeMinute - 1, MIN, 1)));
        assertEquals(evenWholeMinute,
            new java.sql.Timestamp(GMT_CONTEXT.ceilMinute(upperBoundaryEvenWholeMinute, MIN, 1)));
        assertNotEquals(evenWholeMinute,
            new java.sql.Timestamp(GMT_CONTEXT.ceilMinute(upperBoundaryEvenWholeMinute + 1, MIN, 1)));

        java.sql.Timestamp evenWholeMinuteCeil20 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 11:20:0").getTime());
        long lowerBoundaryEvenWholeMinuteCeil20 = evenWholeMinuteCeil20.getTime() - 20 * MIN + 1;
        long upperBoundaryEvenWholeMinuteCeil20 = evenWholeMinuteCeil20.getTime();
        assertEquals(lowerBoundaryEvenWholeMinuteCeil20, GMT_CONTEXT.floorMinute(evenWholeMinute.getTime() -1 , 20 * MIN, 20) + 1);
        assertEquals(upperBoundaryEvenWholeMinuteCeil20, GMT_CONTEXT.ceilMinute(evenWholeMinute.getTime(), 20 * MIN, 20));
        assertEquals(evenWholeMinuteCeil20,
            new java.sql.Timestamp(GMT_CONTEXT.ceilMinute(lowerBoundaryEvenWholeMinuteCeil20, 20 * MIN, 20)));
        assertNotEquals(evenWholeMinuteCeil20,
            new java.sql.Timestamp(GMT_CONTEXT.ceilMinute(lowerBoundaryEvenWholeMinuteCeil20 -1, 20 * MIN, 20)));
        assertEquals(evenWholeMinuteCeil20,
            new java.sql.Timestamp(GMT_CONTEXT.ceilMinute(upperBoundaryEvenWholeMinuteCeil20, 20 * MIN, 20)));
        assertNotEquals(evenWholeMinuteCeil20,
            new java.sql.Timestamp(GMT_CONTEXT.ceilMinute(upperBoundaryEvenWholeMinuteCeil20 + 1, 20 * MIN, 20)));
        
        //Minutes since epoch, don't expect the rounded value to be "round"
        java.sql.Timestamp evenWholeMinuteCeil17 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 11:12:00").getTime());
        long lowerBoundaryEvenWholeMinute17 = evenWholeMinuteCeil17.getTime() - 17 * MIN + 1;
        long upperBoundaryEvenWholeMinute17 = evenWholeMinuteCeil17.getTime();
        assertEquals(lowerBoundaryEvenWholeMinute17, GMT_CONTEXT.floorMinute(evenWholeMinute.getTime() - 1, 17 * MIN, 17) + 1);
        assertEquals(upperBoundaryEvenWholeMinute17, GMT_CONTEXT.ceilMinute(evenWholeMinute.getTime(), 17 * MIN, 17));
        assertEquals(evenWholeMinuteCeil17,
            new java.sql.Timestamp(GMT_CONTEXT.ceilMinute(lowerBoundaryEvenWholeMinute17, 17 * MIN, 17)));
        assertNotEquals(evenWholeMinuteCeil17,
            new java.sql.Timestamp(GMT_CONTEXT.ceilMinute(lowerBoundaryEvenWholeMinute17 -1, 17 * MIN, 17)));
        assertEquals(evenWholeMinuteCeil17,
            new java.sql.Timestamp(GMT_CONTEXT.ceilMinute(upperBoundaryEvenWholeMinute17, 17 * MIN, 17)));
        assertNotEquals(evenWholeMinuteCeil17,
            new java.sql.Timestamp(GMT_CONTEXT.ceilMinute(upperBoundaryEvenWholeMinute17 + 1, 17 * MIN, 17)));

        java.sql.Timestamp oddWholeHour =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 11:0:0").getTime());
        long lowerBoundaryOddWholeHour = oddWholeHour.getTime() - HOUR + 1;
        long upperBoundaryOddWholeHour = oddWholeHour.getTime();
        assertEquals(lowerBoundaryOddWholeHour, GMT_CONTEXT.floorHour(oddWholeHour.getTime() -1, HOUR, 1) + 1);
        assertEquals(upperBoundaryOddWholeHour, GMT_CONTEXT.ceilHour(oddWholeHour.getTime(), HOUR, 1));
        assertEquals(oddWholeHour,
            new java.sql.Timestamp(GMT_CONTEXT.ceilHour(lowerBoundaryOddWholeHour, HOUR, 1)));
        assertNotEquals(oddWholeHour,
            new java.sql.Timestamp(GMT_CONTEXT.ceilHour(lowerBoundaryOddWholeHour - 1, HOUR, 1)));
        assertEquals(oddWholeHour,
            new java.sql.Timestamp(GMT_CONTEXT.ceilHour(upperBoundaryOddWholeHour, HOUR, 1)));
        assertNotEquals(oddWholeHour,
            new java.sql.Timestamp(GMT_CONTEXT.ceilHour(upperBoundaryOddWholeHour + 1, HOUR, 1)));

        //Not rounding to hourOfDay
        java.sql.Timestamp oddWholeHour10 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 12:0:0").getTime());
        long lowerBoundaryOddWholeHour10 = oddWholeHour10.getTime() - 10 * HOUR + 1;
        long upperBoundaryOddWholeHour10 = oddWholeHour10.getTime();
        assertEquals(lowerBoundaryOddWholeHour10, GMT_CONTEXT.floorHour(oddWholeHour.getTime() -1 , 10 * HOUR, 10) + 1);
        assertEquals(upperBoundaryOddWholeHour10, GMT_CONTEXT.ceilHour(oddWholeHour.getTime(), 10 * HOUR, 10));
        assertEquals(oddWholeHour10,
            new java.sql.Timestamp(GMT_CONTEXT.ceilHour(lowerBoundaryOddWholeHour10, 10 * HOUR, 10)));
        assertNotEquals(oddWholeHour10,
            new java.sql.Timestamp(GMT_CONTEXT.ceilHour(lowerBoundaryOddWholeHour10 - 1, 10 * HOUR, 10)));
        assertEquals(oddWholeHour10,
            new java.sql.Timestamp(GMT_CONTEXT.ceilHour(upperBoundaryOddWholeHour10, 10 * HOUR, 10)));
        assertNotEquals(oddWholeHour10,
            new java.sql.Timestamp(GMT_CONTEXT.ceilHour(upperBoundaryOddWholeHour10 + 1, 10 * HOUR, 10)));
        
        //Not rounding to hourOfDay
        java.sql.Timestamp oddWholeHour11 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-11 18:0:0").getTime());
        long lowerBoundaryOddWholeHour11 = oddWholeHour11.getTime() - 11 * HOUR + 1;
        long upperBoundaryOddWholeHour11 = oddWholeHour11.getTime();
        assertEquals(lowerBoundaryOddWholeHour11, GMT_CONTEXT.floorHour(oddWholeHour.getTime() -1, 11 * HOUR, 11) + 1);
        assertEquals(upperBoundaryOddWholeHour11, GMT_CONTEXT.ceilHour(oddWholeHour.getTime(), 11 * HOUR, 11));
        assertEquals(oddWholeHour11,
            new java.sql.Timestamp(GMT_CONTEXT.ceilHour(lowerBoundaryOddWholeHour11, 11 * HOUR, 11)));
        assertNotEquals(oddWholeHour11,
            new java.sql.Timestamp(GMT_CONTEXT.ceilHour(lowerBoundaryOddWholeHour11 - 1, 11 * HOUR, 11)));
        assertEquals(oddWholeHour11,
            new java.sql.Timestamp(GMT_CONTEXT.ceilHour(upperBoundaryOddWholeHour11, 11 * HOUR, 11)));
        assertNotEquals(oddWholeHour11,
            new java.sql.Timestamp(GMT_CONTEXT.ceilHour(upperBoundaryOddWholeHour11 + 1, 11 * HOUR, 11)));

        
        // From Days and larger periods, we're using proper date arithmetics 
        // No DST switchover
        java.sql.Timestamp evenWholeDay =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-12 0:0:0").getTime());
        long lowerBoundaryEvenWholeDay = evenWholeDay.getTime() - DAY + 1;
        long upperBoundaryEvenWholeDay = evenWholeDay.getTime();
        assertEquals(lowerBoundaryEvenWholeDay, GMT_CONTEXT.floorDay(evenWholeDay.getTime() - 1, DAY, 1) + 1);
        assertEquals(upperBoundaryEvenWholeDay, GMT_CONTEXT.ceilDay(evenWholeDay.getTime(), DAY, 1));
        assertEquals(evenWholeDay,
            new java.sql.Timestamp(GMT_CONTEXT.ceilDay(lowerBoundaryEvenWholeDay, DAY, 1)));
        assertNotEquals(evenWholeDay,
            new java.sql.Timestamp(GMT_CONTEXT.ceilDay(lowerBoundaryEvenWholeDay -1, DAY, 1)));
        assertEquals(evenWholeDay,
            new java.sql.Timestamp(GMT_CONTEXT.ceilDay(upperBoundaryEvenWholeDay, DAY, 1)));
        assertNotEquals(evenWholeDay,
            new java.sql.Timestamp(GMT_CONTEXT.ceilDay(upperBoundaryEvenWholeDay + 1, DAY, 1)));

        java.sql.Timestamp evenWholeDay2 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-12 0:0:0").getTime());
        long lowerBoundaryEvenWholeDay2 = evenWholeDay2.getTime() - 2 * DAY + 1;
        long upperBoundaryEvenWholeDay2 = evenWholeDay2.getTime();
        assertEquals(lowerBoundaryEvenWholeDay2, GMT_CONTEXT.floorDay(evenWholeDay.getTime() - 1, 2 * DAY, 2) + 1);
        assertEquals(upperBoundaryEvenWholeDay2, GMT_CONTEXT.ceilDay(evenWholeDay.getTime(), 2 * DAY, 2));
        assertEquals(evenWholeDay2,
            new java.sql.Timestamp(GMT_CONTEXT.ceilDay(lowerBoundaryEvenWholeDay2, 2 * DAY, 2)));
        assertNotEquals(evenWholeDay2,
            new java.sql.Timestamp(GMT_CONTEXT.ceilDay(lowerBoundaryEvenWholeDay2 - 1, 2 * DAY, 2)));
        assertEquals(evenWholeDay2,
            new java.sql.Timestamp(GMT_CONTEXT.ceilDay(upperBoundaryEvenWholeDay2, 2 * DAY, 2)));
        assertNotEquals(evenWholeDay2,
            new java.sql.Timestamp(GMT_CONTEXT.ceilDay(upperBoundaryEvenWholeDay2 + 1, 2 * DAY, 2)));

        java.sql.Timestamp evenWholeDay3 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-12 0:0:0").getTime());
        long lowerBoundaryEvenWholeDay3 = evenWholeDay3.getTime() - 3 * DAY + 1;
        long upperBoundaryEvenWholeDay3 = evenWholeDay3.getTime();
        assertEquals(lowerBoundaryEvenWholeDay3, GMT_CONTEXT.floorDay(evenWholeDay.getTime() -1, 3 * DAY, 3) + 1);
        assertEquals(upperBoundaryEvenWholeDay3, GMT_CONTEXT.ceilDay(evenWholeDay.getTime(), 3 * DAY, 3));
        assertEquals(evenWholeDay3,
            new java.sql.Timestamp(GMT_CONTEXT.ceilDay(lowerBoundaryEvenWholeDay3, 3 * DAY, 3)));
        assertNotEquals(evenWholeDay3,
            new java.sql.Timestamp(GMT_CONTEXT.ceilDay(lowerBoundaryEvenWholeDay3 - 1, 3 * DAY, 3)));
        assertEquals(evenWholeDay3,
            new java.sql.Timestamp(GMT_CONTEXT.ceilDay(upperBoundaryEvenWholeDay3, 3 * DAY, 3)));
        assertNotEquals(evenWholeDay3,
            new java.sql.Timestamp(GMT_CONTEXT.ceilDay(upperBoundaryEvenWholeDay3 + 1, 3 * DAY, 3)));
        
        java.sql.Timestamp wholeDayDst23Odd =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-03-13 0:0:0").getTime());
        //No DST in GMT
        java.sql.Timestamp wholeDayDst23Odd10 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-03-19 0:0:0").getTime());
        long lowerBoundaryEvenWholeDayDst23Odd10 = wholeDayDst23Odd10.getTime() - (10 * DAY) + 1;
        long upperBoundaryEvenWholeDayDst23Odd10 = wholeDayDst23Odd10.getTime();
        assertEquals(lowerBoundaryEvenWholeDayDst23Odd10, GMT_CONTEXT.floorDay(wholeDayDst23Odd.getTime() -1 , 10 * DAY, 10) + 1);
        assertEquals(upperBoundaryEvenWholeDayDst23Odd10, GMT_CONTEXT.ceilDay(wholeDayDst23Odd.getTime(), 10 * DAY, 10));
        assertEquals(wholeDayDst23Odd10,
            new java.sql.Timestamp(GMT_CONTEXT.ceilDay(lowerBoundaryEvenWholeDayDst23Odd10, 10 * DAY, 10)));
        assertNotEquals(wholeDayDst23Odd10,
            new java.sql.Timestamp(GMT_CONTEXT.ceilDay(lowerBoundaryEvenWholeDayDst23Odd10 - 1, 10 * DAY, 10)));
        assertEquals(wholeDayDst23Odd10,
            new java.sql.Timestamp(GMT_CONTEXT.ceilDay(upperBoundaryEvenWholeDayDst23Odd10, 10 * DAY, 10)));
        assertNotEquals(wholeDayDst23Odd10,
            new java.sql.Timestamp(GMT_CONTEXT.ceilDay(upperBoundaryEvenWholeDayDst23Odd10 + 1, 10 * DAY, 10)));
        
        //No DST in GMT
        java.sql.Timestamp wholeDayDst23Odd27 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-04-07 0:0:0").getTime());
        long lowerBoundaryEvenWholeDayDst23Odd27 = wholeDayDst23Odd27.getTime() - (27 * DAY) + 1;
        long upperBoundaryEvenWholeDayDst23Odd27 = wholeDayDst23Odd27.getTime();
        assertEquals(lowerBoundaryEvenWholeDayDst23Odd27, GMT_CONTEXT.floorDay(wholeDayDst23Odd.getTime() - 1, 27 * DAY, 27) + 1 );
        assertEquals(upperBoundaryEvenWholeDayDst23Odd27, GMT_CONTEXT.ceilDay(wholeDayDst23Odd.getTime(), 27 * DAY, 27));
        assertEquals(wholeDayDst23Odd27,
            new java.sql.Timestamp(GMT_CONTEXT.ceilDay(lowerBoundaryEvenWholeDayDst23Odd27, 27 * DAY, 27)));
        assertNotEquals(wholeDayDst23Odd27,
            new java.sql.Timestamp(GMT_CONTEXT.ceilDay(lowerBoundaryEvenWholeDayDst23Odd27 - 1, 27 * DAY, 27)));
        assertEquals(wholeDayDst23Odd27,
            new java.sql.Timestamp(GMT_CONTEXT.ceilDay(upperBoundaryEvenWholeDayDst23Odd27, 27 * DAY, 27)));
        assertNotEquals(wholeDayDst23Odd27,
            new java.sql.Timestamp(GMT_CONTEXT.ceilDay(upperBoundaryEvenWholeDayDst23Odd27 + 1, 27 * DAY, 27)));
        
        //No DST in GMT
        //DST switchover day (12th is 23 hours long)
        java.sql.Timestamp wholeDayDst23 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2023-03-13 0:0:0").getTime());
        long lowerBoundaryEvenWholeDayDst23Even = wholeDayDst23.getTime() - DAY + 1;
        long upperBoundaryEvenWholeDayDst23Even = wholeDayDst23.getTime();
        assertEquals(lowerBoundaryEvenWholeDayDst23Even, GMT_CONTEXT.floorDay(wholeDayDst23.getTime() -1, DAY, 1) + 1);
        assertEquals(upperBoundaryEvenWholeDayDst23Even, GMT_CONTEXT.ceilDay(wholeDayDst23.getTime(), DAY, 1));
        assertEquals(wholeDayDst23,
            new java.sql.Timestamp(GMT_CONTEXT.ceilDay(lowerBoundaryEvenWholeDayDst23Even, DAY, 1)));
        assertNotEquals(wholeDayDst23,
            new java.sql.Timestamp(GMT_CONTEXT.ceilDay(lowerBoundaryEvenWholeDayDst23Even -1, DAY, 1)));
        assertEquals(wholeDayDst23,
            new java.sql.Timestamp(GMT_CONTEXT.ceilDay(upperBoundaryEvenWholeDayDst23Even, DAY, 1)));
        assertNotEquals(wholeDayDst23,
            new java.sql.Timestamp(GMT_CONTEXT.ceilDay(upperBoundaryEvenWholeDayDst23Even + 1, DAY, 1)));

        //No DST in GMT
        //DST switchover day (6th is day is 25 hours long)
        java.sql.Timestamp wholeDayDst25Even =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-07 0:0:0").getTime());
        long lowerBoundaryEvenWholeDayDst25Even = wholeDayDst25Even.getTime() - DAY + 1;
        long upperBoundaryEvenWholeDayDst25Even = wholeDayDst25Even.getTime();
        assertEquals(lowerBoundaryEvenWholeDayDst25Even, GMT_CONTEXT.floorDay(wholeDayDst25Even.getTime() -1, DAY, 1) + 1);
        assertEquals(upperBoundaryEvenWholeDayDst25Even, GMT_CONTEXT.ceilDay(wholeDayDst25Even.getTime(), DAY, 1));
        assertEquals(wholeDayDst25Even,
            new java.sql.Timestamp(GMT_CONTEXT.ceilDay(lowerBoundaryEvenWholeDayDst25Even, DAY, 1)));
        assertNotEquals(wholeDayDst25Even,
            new java.sql.Timestamp(GMT_CONTEXT.ceilDay(lowerBoundaryEvenWholeDayDst25Even -1, DAY, 1)));
        assertEquals(wholeDayDst25Even,
            new java.sql.Timestamp(GMT_CONTEXT.ceilDay(upperBoundaryEvenWholeDayDst25Even, DAY, 1)));
        assertNotEquals(wholeDayDst25Even,
            new java.sql.Timestamp(GMT_CONTEXT.ceilDay(upperBoundaryEvenWholeDayDst25Even + 1, DAY, 1)));

        java.sql.Timestamp wholeWeekOdd =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-10-10 0:0:0").getTime());
        long lowerBoundaryWholeWeekOdd = wholeWeekOdd.getTime() - WEEK + 1;
        long upperBoundaryWholeWeekOdd = wholeWeekOdd.getTime();
        assertEquals(lowerBoundaryWholeWeekOdd, GMT_CONTEXT.floorWeek(wholeWeekOdd.getTime() -1, 0, 1) +1);
        assertEquals(upperBoundaryWholeWeekOdd, GMT_CONTEXT.ceilWeek(wholeWeekOdd.getTime(), 0, 1));
        assertEquals(wholeWeekOdd,
            new java.sql.Timestamp(GMT_CONTEXT.ceilWeek(lowerBoundaryWholeWeekOdd, 0, 1)));
        assertNotEquals(wholeWeekOdd,
            new java.sql.Timestamp(GMT_CONTEXT.ceilWeek(lowerBoundaryWholeWeekOdd -1, 0, 1)));
        assertEquals(wholeWeekOdd,
            new java.sql.Timestamp(GMT_CONTEXT.ceilWeek(upperBoundaryWholeWeekOdd, 0, 1)));
        assertNotEquals(wholeWeekOdd,
            new java.sql.Timestamp(GMT_CONTEXT.ceilWeek(upperBoundaryWholeWeekOdd + 1, 0, 1)));

        //No DST in GMT
        java.sql.Timestamp wholeWeekDst25 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-11-07 0:0:0").getTime());
        long lowerBoundaryWholeWeekDst25Even = wholeWeekDst25.getTime() - WEEK + 1;
        long upperBoundaryWholeWeekDst25Even = wholeWeekDst25.getTime();
        assertEquals(lowerBoundaryWholeWeekDst25Even, GMT_CONTEXT.floorWeek(wholeWeekDst25.getTime() - 1, 0, 1) + 1);
        assertEquals(upperBoundaryWholeWeekDst25Even, GMT_CONTEXT.ceilWeek(wholeWeekDst25.getTime(), 0, 1));
        assertEquals(wholeWeekDst25,
            new java.sql.Timestamp(GMT_CONTEXT.ceilWeek(lowerBoundaryWholeWeekDst25Even, 0, 1)));
        assertNotEquals(wholeWeekDst25,
            new java.sql.Timestamp(GMT_CONTEXT.ceilWeek(lowerBoundaryWholeWeekDst25Even - 1, 0, 1)));
        assertEquals(wholeWeekDst25,
            new java.sql.Timestamp(GMT_CONTEXT.ceilWeek(upperBoundaryWholeWeekDst25Even, 0, 1)));
        assertNotEquals(wholeWeekDst25,
            new java.sql.Timestamp(GMT_CONTEXT.ceilWeek(upperBoundaryWholeWeekDst25Even + 1, 0, 1)));

        // Is it even possible for the switchover to be on an Odd week (of Year) ?
        
        //No DST in GMT
        java.sql.Timestamp wholeWeekDst23 =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-03-14 0:0:0").getTime());
        long lowerBoundaryWholeWeekDst23Even = wholeWeekDst23.getTime() - WEEK + 1;
        long upperBoundaryWholeWeekDst23Even = wholeWeekDst23.getTime();
        assertEquals(lowerBoundaryWholeWeekDst23Even, GMT_CONTEXT.floorWeek(wholeWeekDst23.getTime() - 1, 0, 1) + 1);
        assertEquals(upperBoundaryWholeWeekDst23Even, GMT_CONTEXT.ceilWeek(wholeWeekDst23.getTime(), 0, 1));
        assertEquals(wholeWeekDst23,
            new java.sql.Timestamp(GMT_CONTEXT.ceilWeek(lowerBoundaryWholeWeekDst23Even, 0, 1)));
        assertNotEquals(wholeWeekDst23,
            new java.sql.Timestamp(GMT_CONTEXT.ceilWeek(lowerBoundaryWholeWeekDst23Even -1, 0, 1)));
        assertEquals(wholeWeekDst23,
            new java.sql.Timestamp(GMT_CONTEXT.ceilWeek(upperBoundaryWholeWeekDst23Even, 0, 1)));
        assertNotEquals(wholeWeekDst23,
            new java.sql.Timestamp(GMT_CONTEXT.ceilWeek(upperBoundaryWholeWeekDst23Even + 1, 0, 1)));
        
        // Is it even possible for the switchover to be on an Odd week (of Year) ?

        java.sql.Timestamp wholeMonthOdd =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-08-1 0:0:0").getTime());
        // July is 31 days
        long lowerBoundaryWholeMonthOdd = wholeMonthOdd.getTime() - 31 * DAY + 1;
        long upperBoundaryWholeMonthOdd = wholeMonthOdd.getTime();
        assertEquals(lowerBoundaryWholeMonthOdd, GMT_CONTEXT.floorMonth(wholeMonthOdd.getTime() -1, 0, 1) + 1);
        assertEquals(upperBoundaryWholeMonthOdd, GMT_CONTEXT.ceilMonth(wholeMonthOdd.getTime(), 0, 1));
        assertEquals(wholeMonthOdd,
            new java.sql.Timestamp(GMT_CONTEXT.ceilMonth(lowerBoundaryWholeMonthOdd, 0, 1)));
        assertNotEquals(wholeMonthOdd,
            new java.sql.Timestamp(GMT_CONTEXT.ceilMonth(lowerBoundaryWholeMonthOdd - 1, 0, 1)));
        assertEquals(wholeMonthOdd,
            new java.sql.Timestamp(GMT_CONTEXT.ceilMonth(upperBoundaryWholeMonthOdd, 0, 1)));
        assertNotEquals(wholeMonthOdd,
            new java.sql.Timestamp(GMT_CONTEXT.ceilMonth(upperBoundaryWholeMonthOdd + 1, 0, 1)));

        //NO DST in GMT
        java.sql.Timestamp wholeMonthDst23Odd =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-04-1 0:0:0").getTime());
        // March is 31 days
        long lowerBoundaryWholeMonthDst23 = wholeMonthDst23Odd.getTime() - 31 * DAY + 1;
        long upperBoundaryWholeMonthDst23Odd = wholeMonthDst23Odd.getTime();
        assertEquals(lowerBoundaryWholeMonthDst23, GMT_CONTEXT.floorMonth(wholeMonthDst23Odd.getTime() -1, 0, 1) + 1);
        assertEquals(upperBoundaryWholeMonthDst23Odd, GMT_CONTEXT.ceilMonth(wholeMonthDst23Odd.getTime(), 0, 1));
        assertEquals(wholeMonthDst23Odd,
            new java.sql.Timestamp(GMT_CONTEXT.ceilMonth(lowerBoundaryWholeMonthDst23, 0, 1)));
        assertNotEquals(wholeMonthDst23Odd,
            new java.sql.Timestamp(GMT_CONTEXT.ceilMonth(lowerBoundaryWholeMonthDst23 -1, 0, 1)));
        assertEquals(wholeMonthDst23Odd,
            new java.sql.Timestamp(GMT_CONTEXT.ceilMonth(upperBoundaryWholeMonthDst23Odd, 0, 1)));
        assertNotEquals(wholeMonthDst23Odd,
            new java.sql.Timestamp(GMT_CONTEXT.ceilMonth(upperBoundaryWholeMonthDst23Odd + 1, 0, 1)));

        java.sql.Timestamp wholeMonthLeap =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2024-03-1 0:0:0").getTime());
        // February is is 29 days
        long lowerBoundaryWholeMonthLeap = wholeMonthLeap.getTime() - 29 * DAY + 1;
        long upperBoundaryWholeMonthLeap = wholeMonthLeap.getTime();
        assertEquals(lowerBoundaryWholeMonthLeap, GMT_CONTEXT.floorMonth(wholeMonthLeap.getTime() -1, 0, 1) + 1);
        assertEquals(upperBoundaryWholeMonthLeap, GMT_CONTEXT.ceilMonth(wholeMonthLeap.getTime(), 0, 1));
        assertEquals(wholeMonthLeap,
            new java.sql.Timestamp(GMT_CONTEXT.ceilMonth(lowerBoundaryWholeMonthLeap, 0, 1)));
        assertNotEquals(wholeMonthLeap,
            new java.sql.Timestamp(GMT_CONTEXT.ceilMonth(lowerBoundaryWholeMonthLeap - 1, 0, 1)));
        assertEquals(wholeMonthLeap,
            new java.sql.Timestamp(GMT_CONTEXT.ceilMonth(upperBoundaryWholeMonthLeap, 0, 1)));
        assertNotEquals(wholeMonthLeap,
            new java.sql.Timestamp(GMT_CONTEXT.ceilMonth(upperBoundaryWholeMonthLeap + 1, 0, 1)));

        java.sql.Timestamp wholeYearEven =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2022-1-1 0:0:0").getTime());
        long lowerBoundaryWholeYearEven = wholeYearEven.getTime() - YEAR + 1;
        long upperBoundaryWholeYearEven = wholeYearEven.getTime();
        assertEquals(lowerBoundaryWholeYearEven, GMT_CONTEXT.floorYear(wholeYearEven.getTime() -1, 0, 1) + 1);
        assertEquals(upperBoundaryWholeYearEven, GMT_CONTEXT.ceilYear(wholeYearEven.getTime(), 0, 1));
        assertEquals(wholeYearEven,
            new java.sql.Timestamp(GMT_CONTEXT.ceilYear(lowerBoundaryWholeYearEven, 0, 1)));
        assertNotEquals(wholeYearEven,
            new java.sql.Timestamp(GMT_CONTEXT.ceilYear(lowerBoundaryWholeYearEven -1, 0, 1)));
        assertEquals(wholeYearEven,
            new java.sql.Timestamp(GMT_CONTEXT.ceilYear(upperBoundaryWholeYearEven, 0, 1)));
        assertNotEquals(wholeYearEven,
            new java.sql.Timestamp(GMT_CONTEXT.ceilYear(upperBoundaryWholeYearEven + 1, 0, 1)));

        java.sql.Timestamp wholeYearLeapEven =
                new java.sql.Timestamp(GMT_CONTEXT.parseDate("2025-1-1 0:0:0").getTime());
        long lowerBoundaryWholeYearLeapEven = wholeYearLeapEven.getTime() - (YEAR + DAY) + 1;
        long upperBoundaryWholeYearLeapEven = wholeYearLeapEven.getTime();
        assertEquals(lowerBoundaryWholeYearLeapEven, GMT_CONTEXT.floorYear(wholeYearLeapEven.getTime() -1, 0, 1) + 1);
        assertEquals(upperBoundaryWholeYearLeapEven, GMT_CONTEXT.ceilYear(wholeYearLeapEven.getTime(), 0, 1));
        assertEquals(wholeYearLeapEven,
            new java.sql.Timestamp(GMT_CONTEXT.ceilYear(lowerBoundaryWholeYearLeapEven, 0, 1)));
        assertNotEquals(wholeYearLeapEven,
            new java.sql.Timestamp(GMT_CONTEXT.ceilYear(lowerBoundaryWholeYearLeapEven - 1, 0, 1)));
        assertEquals(wholeYearLeapEven,
            new java.sql.Timestamp(GMT_CONTEXT.ceilYear(upperBoundaryWholeYearLeapEven, 0, 1)));
        assertNotEquals(wholeYearLeapEven,
            new java.sql.Timestamp(GMT_CONTEXT.ceilYear(upperBoundaryWholeYearLeapEven + 1, 0, 1)));
    }

    
}
