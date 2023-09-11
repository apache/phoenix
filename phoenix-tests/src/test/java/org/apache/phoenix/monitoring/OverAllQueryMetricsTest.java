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
package org.apache.phoenix.monitoring;

import org.apache.phoenix.log.LogLevel;
import org.apache.phoenix.util.EnvironmentEdge;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.apache.phoenix.monitoring.MetricType.CACHE_REFRESH_SPLITS_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.NO_OP_METRIC;
import static org.apache.phoenix.monitoring.MetricType.NUM_PARALLEL_SCANS;
import static org.apache.phoenix.monitoring.MetricType.QUERY_FAILED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.QUERY_TIMEOUT_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.RESULT_SET_TIME_MS;
import static org.apache.phoenix.monitoring.MetricType.WALL_CLOCK_TIME_MS;
import static org.junit.Assert.assertEquals;

public class OverAllQueryMetricsTest {

    private OverAllQueryMetrics overAllQueryMetrics;
    private static final long numParallelScans = 10L;
    private static final long delta = 1000L;
    private static final int queryTimeouts = 5;
    private static final int queryFailures = 8;
    private static final int cacheRefreshesDueToSplits = 15;

    @Before
    public void getFreshMetricsObject() {
        overAllQueryMetrics = new OverAllQueryMetrics(true, LogLevel.TRACE);
        populateMetrics(overAllQueryMetrics, numParallelScans, queryTimeouts, queryFailures,
                cacheRefreshesDueToSplits);
    }

    @After
    public void reset() {
        EnvironmentEdgeManager.reset();
    }

    private static class MyClock extends EnvironmentEdge {
        private long time;
        private long delta;

        public MyClock(long time, long delta) {
            this.time = time;
            this.delta = delta;
        }

        @Override public long currentTime() {
            long prevTime = this.time;
            this.time += this.delta;
            return prevTime;
        }
    }

        @Test
    public void testQueryWatchTimer() {
        assertEquals(0L, overAllQueryMetrics.getWallClockTimeMs());
        MyClock clock = new MyClock(10L, delta);
        EnvironmentEdgeManager.injectEdge(clock);
        overAllQueryMetrics.startQuery();
        overAllQueryMetrics.endQuery();
        assertEquals(delta, overAllQueryMetrics.getWallClockTimeMs());
        // Ensure that calling endQuery() again doesn't change the wallClockTimeMs
        overAllQueryMetrics.endQuery();
        assertEquals(delta, overAllQueryMetrics.getWallClockTimeMs());
    }

    @Test
    public void testResultSetWatch() {
        assertEquals(0L, overAllQueryMetrics.getResultSetTimeMs());
        MyClock clock = new MyClock(10L, delta);
        EnvironmentEdgeManager.injectEdge(clock);
        overAllQueryMetrics.startResultSetWatch();
        overAllQueryMetrics.stopResultSetWatch();
        assertEquals(delta, overAllQueryMetrics.getResultSetTimeMs());
        // Ensure that calling stopResultSetWatch() again doesn't change the resultSetTimeMs
        overAllQueryMetrics.stopResultSetWatch();
        assertEquals(delta, overAllQueryMetrics.getResultSetTimeMs());
    }

    @Test
    public void testPublish() {
        MyClock clock = new MyClock(10L, delta);
        EnvironmentEdgeManager.injectEdge(clock);
        overAllQueryMetrics.startQuery();
        overAllQueryMetrics.startResultSetWatch();
        assertPublishedMetrics(overAllQueryMetrics.publish(), numParallelScans, queryTimeouts,
                queryFailures, cacheRefreshesDueToSplits, 0L);
        overAllQueryMetrics.endQuery();
        overAllQueryMetrics.stopResultSetWatch();
        // expect 2 * delta since we call both endQuery() and stopResultSetWatch()
        assertPublishedMetrics(overAllQueryMetrics.publish(), numParallelScans, queryTimeouts,
                queryFailures, cacheRefreshesDueToSplits, 2*delta);
    }

    @Test
    public void testReset() {
        assertPublishedMetrics(overAllQueryMetrics.publish(), numParallelScans, queryTimeouts,
                queryFailures, cacheRefreshesDueToSplits, 0L);
        overAllQueryMetrics.reset();
        assertPublishedMetrics(overAllQueryMetrics.publish(), 0L, 0L, 0L, 0L, 0L);
    }

    @Test
    public void testCombine() {
        OverAllQueryMetrics otherMetrics = new OverAllQueryMetrics(true, LogLevel.TRACE);
        final long otherNumParallelScans = 9L;
        final int otherQueryTimeouts = 8;
        final int otherQueryFailures = 7;
        final int otherCacheRefreshes = 6;
        populateMetrics(otherMetrics, otherNumParallelScans, otherQueryTimeouts, otherQueryFailures,
                otherCacheRefreshes);
        OverAllQueryMetrics finalMetricObj = this.overAllQueryMetrics.combine(otherMetrics);
        assertPublishedMetrics(finalMetricObj.publish(), numParallelScans + otherNumParallelScans,
                queryTimeouts + otherQueryTimeouts, queryFailures + otherQueryFailures,
                cacheRefreshesDueToSplits + otherCacheRefreshes, 0L);
    }

    @Test
    public void testNoOpRequestMetricsIfRequestMetricsDisabled() {
        OverAllQueryMetrics noOpMetrics = new OverAllQueryMetrics(false, LogLevel.OFF);
        populateMetrics(noOpMetrics, numParallelScans, queryTimeouts, queryFailures,
                cacheRefreshesDueToSplits);
        Map<MetricType, Long> noOpMap = noOpMetrics.publish();
        assertEquals(1, noOpMap.size());
        assertEquals(0L, (long)noOpMap.get(NO_OP_METRIC));
    }

    private void populateMetrics(OverAllQueryMetrics metricsObj, long numParallelScansSetting,
            int queryTimeoutsSetting, int queryFailuresSetting,
            int cacheRefreshesDueToSplitsSetting) {
        metricsObj.updateNumParallelScans(numParallelScansSetting);
        for (int i = 0; i < queryTimeoutsSetting; i++) {
            metricsObj.queryTimedOut();
        }
        for (int i = 0; i < queryFailuresSetting; i++) {
            metricsObj.queryFailed();
        }
        for (int i = 0; i < cacheRefreshesDueToSplitsSetting; i++) {
            metricsObj.cacheRefreshedDueToSplits();
        }
    }

    private void assertPublishedMetrics(
            final Map<MetricType, Long> metrics,
            final long expectedNumParallelScans,
            final long expectedQueryTimeouts,
            final long expectedQueryFailures,
            final long expectedCacheRefreshes,
            final long expectedElapsedTime) {
        assertEquals(expectedNumParallelScans, (long)metrics.get(NUM_PARALLEL_SCANS));
        assertEquals(expectedQueryTimeouts, (long)metrics.get(QUERY_TIMEOUT_COUNTER));
        assertEquals(expectedQueryFailures, (long)metrics.get(QUERY_FAILED_COUNTER));
        assertEquals(expectedCacheRefreshes, (long)metrics.get(CACHE_REFRESH_SPLITS_COUNTER));
        assertEquals(expectedElapsedTime, (long)metrics.get(WALL_CLOCK_TIME_MS));
        assertEquals(expectedElapsedTime, (long)metrics.get(RESULT_SET_TIME_MS));
    }

}
