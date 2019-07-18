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

    private final MutableFastCounter indexRepairs;
    private final MutableFastCounter indexRepairFailures;

    private final MetricHistogram indexRepairTimeHisto;
    private final MetricHistogram indexRepairFailureTimeHisto;

    public GlobalIndexCheckerSourceImpl() {
        this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
    }

    public GlobalIndexCheckerSourceImpl(String metricsName,
                                        String metricsDescription,
                                        String metricsContext,
                                        String metricsJmxContext) {
        super(metricsName, metricsDescription, metricsContext, metricsJmxContext);

        indexRepairs = getMetricsRegistry().newCounter(INDEX_REPAIR, INDEX_REPAIR_DESC, 0L);
        indexRepairFailures = getMetricsRegistry().newCounter(INDEX_REPAIR_FAILURE, INDEX_REPAIR_FAILURE_DESC, 0L);

        indexRepairTimeHisto = getMetricsRegistry().newHistogram(INDEX_REPAIR_TIME, INDEX_REPAIR_TIME_DESC);
        indexRepairFailureTimeHisto = getMetricsRegistry().newHistogram(INDEX_REPAIR_FAILURE_TIME, INDEX_REPAIR_FAILURE_TIME_DESC);
    }

    /**
     * Increments the number of index repairs
     */
    public void incrementIndexRepairs() {
        indexRepairs.incr();
    }

    /**
     * Increments the number of index repair failures
     */
    public void incrementIndexRepairFailures() {
        indexRepairFailures.incr();
    }

    /**
     * Updates the index repair time histogram
     *
     * @param t time taken in milliseconds
     */
    public void updateIndexRepairTime(long t) {
        indexRepairTimeHisto.add(t);
    }

    /**
     * Updates the index repair failure time histogram
     *
     * @param t time taken in milliseconds
     */
    public void updateIndexRepairFailureTime(long t) {
        indexRepairFailureTimeHisto.add(t);
    }
}