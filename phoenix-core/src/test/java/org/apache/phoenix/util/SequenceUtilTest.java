/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.phoenix.util;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.junit.Test;

public class SequenceUtilTest {

    private static long MIN_VALUE = 1;
    private static long MAX_VALUE = 10;
    private static long CACHE_SIZE = 2;

    @Test
    public void testAscendingNextValueWithinLimit() throws SQLException {
        assertEquals(9, SequenceUtil.getNextValue(5, MIN_VALUE, MAX_VALUE, 2/* incrementBy */,
            CACHE_SIZE, false));
    }
    
    @Test
    public void testAscendingNextValueReachLimit() throws SQLException {
        assertEquals(MAX_VALUE, SequenceUtil.getNextValue(6, MIN_VALUE, MAX_VALUE, 2/* incrementBy */,
            CACHE_SIZE, false));
    }

    @Test
    public void testAscendingNextValueGreaterThanMaxValueNoCycle() throws SQLException {
        try {
            SequenceUtil.getNextValue(MAX_VALUE, MIN_VALUE, MAX_VALUE, 2/* incrementBy */, CACHE_SIZE,
                false);
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MAX_VALUE.getErrorCode(),
                e.getErrorCode());
        }
    }

    @Test
    public void testAscendingNextValueGreaterThanMaxValueCycle() throws SQLException {
        assertEquals(MIN_VALUE, SequenceUtil.getNextValue(MAX_VALUE, MIN_VALUE, MAX_VALUE,
            2/* incrementBy */, CACHE_SIZE, true));
    }
    
    @Test
    public void testAscendingOverflowNoCycle() throws SQLException {
        try {
            SequenceUtil.getNextValue(Long.MAX_VALUE, 0, Long.MAX_VALUE, 1/* incrementBy */, CACHE_SIZE,
                false);
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MAX_VALUE.getErrorCode(),
                e.getErrorCode());
        }
    }
    
    @Test
    public void testAscendingOverflowCycle() throws SQLException {
        assertEquals(0, SequenceUtil.getNextValue(Long.MAX_VALUE, 0, Long.MAX_VALUE,
            1/* incrementBy */, CACHE_SIZE, true));
    }

    @Test
    public void testDescendingNextValueWithinLimit() throws SQLException {
        assertEquals(2, SequenceUtil.getNextValue(6, MIN_VALUE, MAX_VALUE, -2/* incrementBy */,
            CACHE_SIZE, false));
    }
    
    @Test
    public void testDescendingNextValueReachLimit() throws SQLException {
        assertEquals(MIN_VALUE, SequenceUtil.getNextValue(5, MIN_VALUE, MAX_VALUE, -2/* incrementBy */,
            CACHE_SIZE, false));
    }

    @Test
    public void testDescendingNextValueLessThanMinValueNoCycle() throws SQLException {
        try {
            SequenceUtil.getNextValue(1, MIN_VALUE, MAX_VALUE, -2/* incrementBy */, CACHE_SIZE,
                false);
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MIN_VALUE.getErrorCode(),
                e.getErrorCode());
        }
    }

    @Test
    public void testDescendingNextValueLessThanMinValueCycle() throws SQLException {
        assertEquals(MAX_VALUE, SequenceUtil.getNextValue(2, MIN_VALUE, MAX_VALUE,
            -2/* incrementBy */, CACHE_SIZE, true));
    }
    
    @Test
    public void testDescendingOverflowNoCycle() throws SQLException {
        try {
            SequenceUtil.getNextValue(Long.MIN_VALUE, Long.MIN_VALUE, 0, -1/* incrementBy */, CACHE_SIZE,
                false);
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MIN_VALUE.getErrorCode(),
                e.getErrorCode());
        }
    }
    
    @Test
    public void testDescendingOverflowCycle() throws SQLException {
        assertEquals(0, SequenceUtil.getNextValue(Long.MIN_VALUE, Long.MIN_VALUE, 0,
            -1/* incrementBy */, CACHE_SIZE, true));
    }
}
