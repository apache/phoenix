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

package org.apache.phoenix.schema.metrics;

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;

public class MetricsMetadataSourceImpl extends BaseSourceImpl implements MetricsMetadataSource {

    private final MutableFastCounter createExportCount;
    private final MetricHistogram createExportTimeHisto;

    private final MutableFastCounter createExportFailureCount;
    private final MetricHistogram createExportFailureTimeHisto;

    private final MutableFastCounter alterExportCount;
    private final MetricHistogram alterExportTimeHisto;

    private final MutableFastCounter alterExportFailureCount;
    private final MetricHistogram alterExportFailureTimeHisto;

    public MetricsMetadataSourceImpl() {
        this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
    }

    public MetricsMetadataSourceImpl(String metricsName, String metricsDescription,
        String metricsContext, String metricsJmxContext) {
        super(metricsName, metricsDescription, metricsContext, metricsJmxContext);

        createExportCount = getMetricsRegistry().newCounter(CREATE_EXPORT_COUNT,
            CREATE_EXPORT_COUNT_DESC, 0L);
        createExportTimeHisto = getMetricsRegistry().newHistogram(CREATE_EXPORT_TIME, CREATE_EXPORT_TIME_DESC);

        createExportFailureCount = getMetricsRegistry().newCounter(CREATE_EXPORT_FAILURE_COUNT,
            CREATE_EXPORT_FAILURE_COUNT_DESC, 0L);
        createExportFailureTimeHisto = getMetricsRegistry().newHistogram(CREATE_EXPORT_FAILURE_TIME,
            CREATE_EXPORT_FAILURE_TIME_DESC);

        alterExportCount = getMetricsRegistry().newCounter(ALTER_EXPORT_COUNT,
            ALTER_EXPORT_COUNT_DESC, 0L);
        alterExportTimeHisto = getMetricsRegistry().newHistogram(ALTER_EXPORT_TIME, ALTER_EXPORT_TIME_DESC);

        alterExportFailureCount = getMetricsRegistry().newCounter(ALTER_EXPORT_FAILURE_COUNT,
            ALTER_EXPORT_FAILURE_COUNT_DESC, 0L);
        alterExportFailureTimeHisto = getMetricsRegistry().newHistogram(ALTER_EXPORT_FAILURE_TIME,
            ALTER_EXPORT_FAILURE_TIME_DESC);

    }

    @Override public void incrementCreateExportCount() {
        createExportCount.incr();
    }

    @Override public void updateCreateExportTime(long t) {
        createExportTimeHisto.add(t);
    }

    @Override public void incrementCreateExportFailureCount() {
        createExportFailureCount.incr();
    }

    @Override public void updateCreateExportFailureTime(long t) {
        createExportFailureTimeHisto.add(t);
    }

    @Override public void incrementAlterExportCount() {
        alterExportCount.incr();
    }

    @Override public void updateAlterExportTime(long t) {
        alterExportTimeHisto.add(t);
    }

    @Override public void incrementAlterExportFailureCount() {
        alterExportFailureCount.incr();
    }

    @Override public void updateAlterExportFailureTime(long t) {
        alterExportFailureTimeHisto.add(t);
    }
}
