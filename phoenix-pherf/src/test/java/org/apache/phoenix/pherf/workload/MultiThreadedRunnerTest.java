/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.phoenix.pherf.workload;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.pherf.configuration.Query;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.configuration.XMLConfigParser;
import org.apache.phoenix.pherf.result.DataModelResult;
import org.apache.phoenix.pherf.result.ThreadTime;
import org.apache.phoenix.pherf.rules.RulesApplier;
import org.apache.phoenix.util.DefaultEnvironmentEdge;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.ResultSet;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MultiThreadedRunnerTest {
    @Mock
    private static XMLConfigParser mockParser;
    @Mock
    private static DataModelResult mockDMR;
    @Mock
    private static RulesApplier mockRA;
    @Mock
    private static ThreadTime mockTT;
    @Mock
    private static Scenario mockScenario;
    @Mock
    private static WorkloadExecutor mockWE;
    @Mock
    private static Query mockQuery;
    @Mock
    private static ResultSet mockRS;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testExpectedRowsMismatch() throws Exception {
        Mockito.when(mockQuery.getExpectedAggregateRowCount()).thenReturn(1L);
        MultiThreadedRunner mtr = new MultiThreadedRunner("test",
                mockQuery, mockDMR, mockTT,
                10L, 1000L,
                true, mockRA,
                mockScenario, mockWE, mockParser);
        Mockito.when(mockRS.next()).thenReturn(true);
        Mockito.when(mockRS.getLong(1)).thenReturn(2L);
        try {
            mtr.getResults(mockRS, "test_iteration", false,0L);
            fail();
        } catch (RuntimeException e) {
            //pass;
        }

    }

    @Test
    public void testTimeout() throws Exception {
        Mockito.when(mockQuery.getTimeoutDuration()).thenReturn(1000L);
        Mockito.when(mockQuery.getExpectedAggregateRowCount()).thenReturn(1L);
        MultiThreadedRunner mtr = new MultiThreadedRunner("test",
                mockQuery, mockDMR, mockTT,
                10L, 1000L,
                true, mockRA,
                mockScenario, mockWE, mockParser);
        DefaultEnvironmentEdge myClock = Mockito.mock(DefaultEnvironmentEdge.class);
        Mockito.when(myClock.currentTime()).thenReturn(0L, 5000L);
        EnvironmentEdgeManager.injectEdge(myClock);
        try {
            Mockito.when(mockRS.next()).thenReturn(true);
            Mockito.when(mockRS.getLong(1)).thenReturn(1L);
            Pair<Long, Long> results = mtr.getResults(mockRS, "test_iteration", false, 0L);
            assertTrue(results.getSecond() > mockQuery.getTimeoutDuration());
        } finally {
            EnvironmentEdgeManager.reset();
        }
    }

    @Test
    public void testFinishWithoutTimeout() throws Exception {
        DefaultEnvironmentEdge myClock = Mockito.mock(DefaultEnvironmentEdge.class);
        Mockito.when(myClock.currentTime()).thenReturn(0L);
        EnvironmentEdgeManager.injectEdge(myClock);
        try {
            Mockito.when(mockQuery.getTimeoutDuration()).thenReturn(1000L);
            Mockito.when(mockQuery.getExpectedAggregateRowCount()).thenReturn(1L);
            MultiThreadedRunner mtr = new MultiThreadedRunner("test",
                    mockQuery, mockDMR, mockTT,
                    10L, 1000L,
                    true, mockRA,
                    mockScenario, mockWE, mockParser);
            Mockito.when(mockRS.next()).thenReturn(true, false);
            Mockito.when(mockRS.getLong(1)).thenReturn(1L);
            Pair<Long, Long> results = mtr.getResults(mockRS, "test_iteration", false, 0L);
            assertFalse(results.getSecond() > mockQuery.getTimeoutDuration());
        } finally {
            EnvironmentEdgeManager.reset();
        }
    }

}
