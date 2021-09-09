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

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.metrics2.lib.DynamicMetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.hadoop.metrics2.lib.MutableHistogram;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.hbase.index.metrics.GlobalIndexCheckerSource;
import org.apache.phoenix.hbase.index.metrics.GlobalIndexCheckerSourceImpl;
import org.apache.phoenix.hbase.index.metrics.MetricsIndexerSource;
import org.apache.phoenix.hbase.index.metrics.MetricsIndexerSourceImpl;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;

import static org.junit.Assert.assertEquals;

@Category(ParallelStatsDisabledTest.class)
public class IndexMetricsIT extends ParallelStatsDisabledIT {
    private static final String TABLE_NAME = "MyTable";
    private static final String INDEX_NAME = "MyIndex";
    public static final int TIME_VAL = 10;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(3);
        props.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB, Long.toString(Long.MAX_VALUE));
        // disable renewing leases as this will force spooling to happen.
        props.put(QueryServices.RENEW_LEASE_ENABLED, String.valueOf(false));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));

    }

    @Test
    public void testIndexRegionObserverCounterMetrics() {
        MetricsIndexerSourceImpl metricSource = new MetricsIndexerSourceImpl();
        DynamicMetricsRegistry registry = metricSource.getMetricsRegistry();

        metricSource.incrementNumSlowIndexPrepareCalls(TABLE_NAME);
        verifyCounter(MetricsIndexerSource.SLOW_INDEX_PREPARE, registry);
        verifyCounter(getTableCounterName(MetricsIndexerSource.SLOW_INDEX_PREPARE), registry);

        metricSource.incrementNumSlowIndexWriteCalls(TABLE_NAME);
        verifyCounter(MetricsIndexerSource.SLOW_INDEX_WRITE, registry);
        verifyCounter(getTableCounterName(MetricsIndexerSource.SLOW_INDEX_WRITE), registry);

        metricSource.incrementNumSlowPostDeleteCalls(TABLE_NAME);
        verifyCounter(MetricsIndexerSource.SLOW_POST_DELETE, registry);
        verifyCounter(getTableCounterName(MetricsIndexerSource.SLOW_POST_DELETE), registry);

        metricSource.incrementNumSlowPostOpenCalls(TABLE_NAME);
        verifyCounter(MetricsIndexerSource.SLOW_POST_OPEN, registry);
        verifyCounter(getTableCounterName(MetricsIndexerSource.SLOW_POST_OPEN), registry);

        metricSource.incrementNumSlowPostPutCalls(TABLE_NAME);
        verifyCounter(MetricsIndexerSource.SLOW_POST_PUT, registry);
        verifyCounter(getTableCounterName(MetricsIndexerSource.SLOW_POST_PUT), registry);

        metricSource.incrementNumSlowPreWALRestoreCalls(TABLE_NAME);
        verifyCounter(MetricsIndexerSource.SLOW_PRE_WAL_RESTORE, registry);
        verifyCounter(getTableCounterName(MetricsIndexerSource.SLOW_PRE_WAL_RESTORE), registry);

        metricSource.incrementPostIndexUpdateFailures(TABLE_NAME);
        verifyCounter(MetricsIndexerSource.POST_INDEX_UPDATE_FAILURE, registry);
        verifyCounter(getTableCounterName(MetricsIndexerSource.POST_INDEX_UPDATE_FAILURE), registry);

        metricSource.incrementPreIndexUpdateFailures(TABLE_NAME);
        verifyCounter(MetricsIndexerSource.PRE_INDEX_UPDATE_FAILURE, registry);
        verifyCounter(getTableCounterName(MetricsIndexerSource.PRE_INDEX_UPDATE_FAILURE), registry);

        metricSource.incrementSlowDuplicateKeyCheckCalls(TABLE_NAME);
        verifyCounter(MetricsIndexerSource.SLOW_DUPLICATE_KEY, registry);
        verifyCounter(getTableCounterName(MetricsIndexerSource.SLOW_DUPLICATE_KEY), registry);

    }

    @Test
    public void testIndexRegionObserverHistogramMetrics() {
        MetricsIndexerSourceImpl metricSource = new MetricsIndexerSourceImpl();
        DynamicMetricsRegistry registry = metricSource.getMetricsRegistry();

        metricSource.updateDuplicateKeyCheckTime(TABLE_NAME, TIME_VAL);
        verifyHistogram(MetricsIndexerSource.DUPLICATE_KEY_TIME, registry);
        verifyHistogram(getTableCounterName(MetricsIndexerSource.DUPLICATE_KEY_TIME), registry);

        metricSource.updateIndexPrepareTime(TABLE_NAME, TIME_VAL);
        verifyHistogram(MetricsIndexerSource.INDEX_PREPARE_TIME, registry);
        verifyHistogram(getTableCounterName(MetricsIndexerSource.INDEX_PREPARE_TIME), registry);

        metricSource.updateIndexWriteTime(TABLE_NAME, TIME_VAL);
        verifyHistogram(MetricsIndexerSource.INDEX_WRITE_TIME, registry);
        verifyHistogram(getTableCounterName(MetricsIndexerSource.INDEX_WRITE_TIME), registry);

        metricSource.updatePostDeleteTime(TABLE_NAME, TIME_VAL);
        verifyHistogram(MetricsIndexerSource.POST_DELETE_TIME, registry);
        verifyHistogram(getTableCounterName(MetricsIndexerSource.POST_DELETE_TIME), registry);

        metricSource.updatePostIndexUpdateTime(TABLE_NAME, TIME_VAL);
        verifyHistogram(MetricsIndexerSource.POST_INDEX_UPDATE_TIME, registry);
        verifyHistogram(getTableCounterName(MetricsIndexerSource.POST_INDEX_UPDATE_TIME), registry);

        metricSource.updatePostIndexUpdateFailureTime(TABLE_NAME, TIME_VAL);
        verifyHistogram(MetricsIndexerSource.POST_INDEX_UPDATE_FAILURE_TIME, registry);
        verifyHistogram(getTableCounterName(MetricsIndexerSource.POST_INDEX_UPDATE_FAILURE_TIME),
            registry);

        metricSource.updatePostOpenTime(TABLE_NAME, TIME_VAL);
        verifyHistogram(MetricsIndexerSource.POST_OPEN_TIME, registry);
        verifyHistogram(getTableCounterName(MetricsIndexerSource.POST_OPEN_TIME), registry);

        metricSource.updatePostPutTime(TABLE_NAME, TIME_VAL);
        verifyHistogram(MetricsIndexerSource.POST_PUT_TIME, registry);
        verifyHistogram(getTableCounterName(MetricsIndexerSource.POST_PUT_TIME), registry);

        metricSource.updatePreIndexUpdateTime(TABLE_NAME, TIME_VAL);
        verifyHistogram(MetricsIndexerSource.PRE_INDEX_UPDATE_TIME, registry);
        verifyHistogram(getTableCounterName(MetricsIndexerSource.PRE_INDEX_UPDATE_TIME), registry);

        metricSource.updatePreIndexUpdateFailureTime(TABLE_NAME, TIME_VAL);
        verifyHistogram(MetricsIndexerSource.POST_INDEX_UPDATE_FAILURE_TIME, registry);
        verifyHistogram(getTableCounterName(MetricsIndexerSource.POST_INDEX_UPDATE_FAILURE_TIME),
            registry);

        metricSource.updatePreWALRestoreTime(TABLE_NAME, TIME_VAL);
        verifyHistogram(MetricsIndexerSource.PRE_WAL_RESTORE_TIME, registry);
        verifyHistogram(getTableCounterName(MetricsIndexerSource.PRE_WAL_RESTORE_TIME), registry);

    }

    @Test
    public void testGlobalIndexCheckerCounterMetrics() {
        GlobalIndexCheckerSourceImpl metricSource = new GlobalIndexCheckerSourceImpl();
        DynamicMetricsRegistry registry = metricSource.getMetricsRegistry();

        metricSource.incrementIndexInspections(INDEX_NAME);
        verifyCounter(GlobalIndexCheckerSource.INDEX_INSPECTION, registry);
        verifyCounter(getIndexCounterName(GlobalIndexCheckerSource.INDEX_INSPECTION), registry);

        metricSource.incrementIndexRepairFailures(INDEX_NAME);
        verifyCounter(GlobalIndexCheckerSource.INDEX_REPAIR_FAILURE, registry);
        verifyCounter(getIndexCounterName(GlobalIndexCheckerSource.INDEX_REPAIR_FAILURE), registry);

        metricSource.incrementIndexRepairs(INDEX_NAME);
        verifyCounter(GlobalIndexCheckerSource.INDEX_REPAIR, registry);
        verifyCounter(getIndexCounterName(GlobalIndexCheckerSource.INDEX_REPAIR), registry);
    }

    @Test
    public void testGlobalIndexCheckerHistogramMetrics() {
        GlobalIndexCheckerSourceImpl metricSource = new GlobalIndexCheckerSourceImpl();
        DynamicMetricsRegistry registry = metricSource.getMetricsRegistry();

        metricSource.updateIndexRepairTime(INDEX_NAME, TIME_VAL);
        verifyHistogram(GlobalIndexCheckerSource.INDEX_REPAIR_TIME, registry);
        verifyHistogram(getIndexCounterName(GlobalIndexCheckerSource.INDEX_REPAIR_TIME),
            registry);

        metricSource.updateIndexRepairFailureTime(INDEX_NAME, TIME_VAL);
        verifyHistogram(GlobalIndexCheckerSource.INDEX_REPAIR_FAILURE_TIME, registry);
        verifyHistogram(getIndexCounterName(GlobalIndexCheckerSource.INDEX_REPAIR_FAILURE_TIME),
            registry);

        long ageOfUnverifiedRow = EnvironmentEdgeManager.currentTimeMillis() - TIME_VAL;
        metricSource.updateUnverifiedIndexRowAge(INDEX_NAME, ageOfUnverifiedRow);
        verifyHistogram(GlobalIndexCheckerSource.UNVERIFIED_INDEX_ROW_AGE, registry,
            ageOfUnverifiedRow);
        verifyHistogram(getIndexCounterName(GlobalIndexCheckerSource.UNVERIFIED_INDEX_ROW_AGE),
            registry, ageOfUnverifiedRow);
    }

    private void verifyHistogram(String counterName, DynamicMetricsRegistry registry) {
        verifyHistogram(counterName, registry, TIME_VAL);
    }

    private void verifyHistogram(String counterName,
            DynamicMetricsRegistry registry, long max) {
        MutableHistogram histogram = registry.getHistogram(counterName);
        assertEquals(max, histogram.getMax());
    }

    private void verifyCounter(String counterName, DynamicMetricsRegistry registry) {
        MutableFastCounter counter = registry.getCounter(counterName, 0);
        assertEquals(1, counter.value());
    }

    private String getTableCounterName(String baseCounterName) {
        return baseCounterName + "." + TABLE_NAME;
    }

    private String getIndexCounterName(String baseCounterName) {
        return baseCounterName + "." + INDEX_NAME;
    }
}
