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

import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;

public class GlobalPhoenixMetricsTestSink implements MetricsSink {

    public static final String PHOENIX_METRICS_RECORD_NAME = "PHOENIX";
    // PhoenixMetricsIT tests verifies these metrics from this sink in a separate thread
    // GlobalPhoenixMetricsTestSink is invoked based on time defined in hadoop-metrics2.properties
    // This lock is to prevent concurrent access to metrics Iterable for these threads
    static final Object lock = new Object();
    static Iterable<AbstractMetric> metrics;

    @Override
    public void putMetrics(MetricsRecord metricsRecord) {
        if (metricsRecord.name().equals(PHOENIX_METRICS_RECORD_NAME)) {
            synchronized (GlobalPhoenixMetricsTestSink.lock) {
                GlobalPhoenixMetricsTestSink.metrics = metricsRecord.metrics();
            }
        }
    }

    @Override
    public void flush() {
    }

    @Override
    public void init(SubsetConfiguration subsetConfiguration) {
    }

}
