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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;

import org.junit.Test;

public class SequenceUtilTest {

    private static long MIN_VALUE = 1;
    private static long MAX_VALUE = 10;
    private static long CACHE_SIZE = 2;

    @Test
    public void testAscendingNextValueWithinLimit() throws SQLException {
        assertFalse(SequenceUtil.checkIfLimitReached(5, MIN_VALUE, MAX_VALUE, 2/* incrementBy */, CACHE_SIZE));
    }
    
    @Test
    public void testAscendingNextValueReachLimit() throws SQLException {
    	assertFalse(SequenceUtil.checkIfLimitReached(6, MIN_VALUE, MAX_VALUE, 2/* incrementBy */,  CACHE_SIZE));
    }

    @Test
    public void testAscendingNextValueGreaterThanMaxValue() throws SQLException {
        assertTrue(SequenceUtil.checkIfLimitReached(MAX_VALUE, MIN_VALUE, MAX_VALUE, 2/* incrementBy */, CACHE_SIZE));
    }
    
    @Test
    public void testAscendingOverflow() throws SQLException {
        assertTrue(SequenceUtil.checkIfLimitReached(Long.MAX_VALUE, 0, Long.MAX_VALUE, 1/* incrementBy */, CACHE_SIZE));
    }

    @Test
    public void testDescendingNextValueWithinLimit() throws SQLException {
    	assertFalse(SequenceUtil.checkIfLimitReached(6, MIN_VALUE, MAX_VALUE, -2/* incrementBy */, CACHE_SIZE));
    }
    
    @Test
    public void testDescendingNextValueReachLimit() throws SQLException {
    	assertFalse(SequenceUtil.checkIfLimitReached(5, MIN_VALUE, MAX_VALUE, -2/* incrementBy */, CACHE_SIZE));
    }

    @Test
    public void testDescendingNextValueLessThanMinValue() throws SQLException {
    	assertTrue(SequenceUtil.checkIfLimitReached(2, MIN_VALUE, MAX_VALUE, -2/* incrementBy */, CACHE_SIZE));
    }
    
    @Test
    public void testDescendingOverflowCycle() throws SQLException {
    	assertTrue(SequenceUtil.checkIfLimitReached(Long.MIN_VALUE, Long.MIN_VALUE, 0, -1/* incrementBy */, CACHE_SIZE));
    }
}
