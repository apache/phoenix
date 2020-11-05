/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hbase.index.metrics;

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;

/**
 * Implementation for tracking Phoenix Index Checker metrics.
 */
public class GlobalIndexCheckerSourceImpl extends BaseSourceImpl implements GlobalIndexCheckerSource {

    private final MutableFastCounter indexInspections;
    private final MutableFastCounter indexRepairs;
    private final MutableFastCounter indexRepairFailures;

    private final MetricHistogram indexRepairTimeHisto;
    private final MetricHistogram indexRepairFailureTimeHisto;
    private final MetricHistogram unverifiedIndexRowAge;

    public GlobalIndexCheckerSourceImpl() {
        this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
    }

    public GlobalIndexCheckerSourceImpl(String metricsName,
                                        String metricsDescription,
                                        String metricsContext,
                                        String metricsJmxContext) {
        super(metricsName, metricsDescription, metricsContext, metricsJmxContext);

        indexInspections = getMetricsRegistry().newCounter(INDEX_INSPECTION, INDEX_INSPECTION_DESC, 0L);
        indexRepairs = getMetricsRegistry().newCounter(INDEX_REPAIR, INDEX_REPAIR_DESC, 0L);
        indexRepairFailures = getMetricsRegistry().newCounter(INDEX_REPAIR_FAILURE, INDEX_REPAIR_FAILURE_DESC, 0L);

        indexRepairTimeHisto = getMetricsRegistry().newHistogram(INDEX_REPAIR_TIME, INDEX_REPAIR_TIME_DESC);
        indexRepairFailureTimeHisto = getMetricsRegistry().newHistogram(INDEX_REPAIR_FAILURE_TIME, INDEX_REPAIR_FAILURE_TIME_DESC);
        unverifiedIndexRowAge = getMetricsRegistry().newHistogram(
            UNVERIFIED_INDEX_ROW_AGE, UNVERIFIED_INDEX_ROW_AGE_DESC);
    }

    /**
     * Increments the number of index rows inspected for verified status
     */
    public void incrementIndexInspections(String indexName) {
        incrementIndexSpecificCounter(INDEX_INSPECTION, indexName);
        indexInspections.incr();
    }

    /**
     * Increments the number of index repairs
     */
    public void incrementIndexRepairs(String indexName) {
        incrementIndexSpecificCounter(INDEX_REPAIR, indexName);
        indexRepairs.incr();
    }

    /**
     * Increments the number of index repair failures
     */
    public void incrementIndexRepairFailures(String indexName) {
        incrementIndexSpecificCounter(INDEX_REPAIR_FAILURE, indexName);
        indexRepairFailures.incr();
    }

    /**
     * Updates the index age of unverified row histogram
     * @param indexName name of the index
     * @param time time taken in milliseconds
     */
    public void updateUnverifiedIndexRowAge(final String indexName,
            final long time) {
        incrementIndexSpecificHistogram(UNVERIFIED_INDEX_ROW_AGE, indexName,
            time);
        unverifiedIndexRowAge.add(time);
    }

    /**
     * Updates the index repair time histogram
     *
     * @param t time taken in milliseconds
     */
    public void updateIndexRepairTime(String indexName, long t) {
        incrementIndexSpecificHistogram(INDEX_REPAIR_TIME, indexName, t);
        indexRepairTimeHisto.add(t);
    }

    /**
     * Updates the index repair failure time histogram
     *
     * @param t time taken in milliseconds
     */
    public void updateIndexRepairFailureTime(String indexName, long t) {
        incrementIndexSpecificHistogram(INDEX_REPAIR_FAILURE_TIME, indexName, t);
        indexRepairFailureTimeHisto.add(t);
    }

    private void incrementIndexSpecificCounter(String baseCounterName, String indexName) {
        MutableFastCounter indexSpecificCounter =
            getMetricsRegistry().getCounter(getCounterName(baseCounterName, indexName), 0);
        indexSpecificCounter.incr();
    }

    private void incrementIndexSpecificHistogram(String baseCounterName, String indexName, long t) {
        MetricHistogram indexSpecificHistogram =
            getMetricsRegistry().getHistogram(getCounterName(baseCounterName, indexName));
        indexSpecificHistogram.add(t);
    }

    private String getCounterName(String baseCounterName, String indexName) {
        return baseCounterName + "." + indexName;
    }
}