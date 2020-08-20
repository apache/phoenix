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
package org.apache.phoenix.coprocessor;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.lang.reflect.Field;

/**
 * Test UngroupedAggregateRegionObserver
 */
public class UngroupedAggregateRegionObserverTest {

    /**
     * Tests RollBackSplit
     * preRollBackSplit
     */
    @Test
    public void testSplitFailedRollBack() throws Exception {
        Class<?> ungroupClass = (Class<UngroupedAggregateRegionObserver>) Class.forName("org.apache.phoenix.coprocessor.UngroupedAggregateRegionObserver");
        Object observer = ungroupClass.newInstance();
        UngroupedAggregateRegionObserver ungroupObserver = (UngroupedAggregateRegionObserver) observer;
        ungroupObserver.preClose(null, true);
        ungroupObserver.preRollBackSplit(null);
        Field fieldTag = ungroupClass.getDeclaredField("isRegionClosingOrSplitting");
        fieldTag.setAccessible(true);
        Boolean isRegionClosingOrSplitting = (Boolean) fieldTag.get(observer);
        assertEquals(isRegionClosingOrSplitting, false);
    }
}

