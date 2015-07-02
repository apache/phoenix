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

import static org.apache.phoenix.monitoring.MetricType.CACHE_REFRESH_SPLITS_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.NUM_PARALLEL_SCANS;
import static org.apache.phoenix.monitoring.MetricType.QUERY_FAILED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.QUERY_TIMEOUT_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.RESULT_SET_TIME_MS;
import static org.apache.phoenix.monitoring.MetricType.WALL_CLOCK_TIME_MS;

import java.util.HashMap;
import java.util.Map;

import org.apache.phoenix.monitoring.CombinableMetric.NoOpRequestMetric;

/**
 * Class that represents the overall metrics associated with a query being executed by the phoenix.
 */
public class OverAllQueryMetrics {
    private final MetricsStopWatch queryWatch;
    private final MetricsStopWatch resultSetWatch;
    private final CombinableMetric numParallelScans;
    private final CombinableMetric wallClockTimeMS;
    private final CombinableMetric resultSetTimeMS;
    private final CombinableMetric queryTimedOut;
    private final CombinableMetric queryFailed;
    private final CombinableMetric cacheRefreshedDueToSplits;

    public OverAllQueryMetrics(boolean isMetricsEnabled) {
        queryWatch = new MetricsStopWatch(isMetricsEnabled);
        resultSetWatch = new MetricsStopWatch(isMetricsEnabled);
        numParallelScans = isMetricsEnabled ? new CombinableMetricImpl(NUM_PARALLEL_SCANS) : NoOpRequestMetric.INSTANCE;
        wallClockTimeMS = isMetricsEnabled ? new CombinableMetricImpl(WALL_CLOCK_TIME_MS) : NoOpRequestMetric.INSTANCE;
        resultSetTimeMS = isMetricsEnabled ? new CombinableMetricImpl(RESULT_SET_TIME_MS) : NoOpRequestMetric.INSTANCE;
        queryTimedOut = isMetricsEnabled ? new CombinableMetricImpl(QUERY_TIMEOUT_COUNTER) : NoOpRequestMetric.INSTANCE;
        queryFailed = isMetricsEnabled ? new CombinableMetricImpl(QUERY_FAILED_COUNTER) : NoOpRequestMetric.INSTANCE;
        cacheRefreshedDueToSplits = isMetricsEnabled ? new CombinableMetricImpl(CACHE_REFRESH_SPLITS_COUNTER)
                : NoOpRequestMetric.INSTANCE;
    }

    public void updateNumParallelScans(long numParallelScans) {
        this.numParallelScans.change(numParallelScans);
    }

    public void queryTimedOut() {
        queryTimedOut.increment();
    }

    public void queryFailed() {
        queryFailed.increment();
    }

    public void cacheRefreshedDueToSplits() {
        cacheRefreshedDueToSplits.increment();
    }

    public void startQuery() {
        queryWatch.start();
    }

    public void endQuery() {
        queryWatch.stop();
        wallClockTimeMS.change(queryWatch.getElapsedTimeInMs());
    }

    public void startResultSetWatch() {
        resultSetWatch.start();
    }

    public void stopResultSetWatch() {
        resultSetWatch.stop();
        resultSetTimeMS.change(resultSetWatch.getElapsedTimeInMs());
    }

    public Map<String, Long> publish() {
        Map<String, Long> metricsForPublish = new HashMap<>();
        metricsForPublish.put(numParallelScans.getName(), numParallelScans.getValue());
        metricsForPublish.put(wallClockTimeMS.getName(), wallClockTimeMS.getValue());
        metricsForPublish.put(resultSetTimeMS.getName(), resultSetTimeMS.getValue());
        metricsForPublish.put(queryTimedOut.getName(), queryTimedOut.getValue());
        metricsForPublish.put(queryFailed.getName(), queryFailed.getValue());
        metricsForPublish.put(cacheRefreshedDueToSplits.getName(), cacheRefreshedDueToSplits.getValue());
        return metricsForPublish;
    }

    public void reset() {
        numParallelScans.reset();
        wallClockTimeMS.reset();
        resultSetTimeMS.reset();
        queryTimedOut.reset();
        queryFailed.reset();
        cacheRefreshedDueToSplits.reset();
        queryWatch.stop();
        resultSetWatch.stop();
    }

    public OverAllQueryMetrics combine(OverAllQueryMetrics metric) {
        cacheRefreshedDueToSplits.combine(metric.cacheRefreshedDueToSplits);
        queryFailed.combine(metric.queryFailed);
        queryTimedOut.combine(metric.queryTimedOut);
        numParallelScans.combine(metric.numParallelScans);
        return this;
    }

}
