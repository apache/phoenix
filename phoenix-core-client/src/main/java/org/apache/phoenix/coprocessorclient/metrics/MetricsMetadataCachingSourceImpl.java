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
package org.apache.phoenix.coprocessorclient.metrics;

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;

/**
 * Implementation for tracking Distributed Metadata Caching metrics.
 */
public class MetricsMetadataCachingSourceImpl
        extends BaseSourceImpl
        implements MetricsMetadataCachingSource {

    private final MutableFastCounter cacheHitCounter;
    private final MutableFastCounter cacheMissCounter;
    private final MutableFastCounter validateDDLTimestampRequestCounter;
    private final MutableFastCounter cacheInvalidationOpsCounter;
    private final MutableFastCounter cacheInvalidationSuccessCounter;
    private final MutableFastCounter cacheInvalidationFailureCounter;
    private final MetricHistogram cacheInvalidationRpcTimeHistogram;
    private final MetricHistogram cacheInvalidationTotalTimeHistogram;

    public MetricsMetadataCachingSourceImpl() {
        this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
    }

    public MetricsMetadataCachingSourceImpl(String metricsName,
                                       String metricsDescription,
                                       String metricsContext,
                                       String metricsJmxContext) {
        super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
        cacheHitCounter = getMetricsRegistry().newCounter(
                REGIONSERVER_METADATA_CACHE_HITS, REGIONSERVER_METADATA_CACHE_HITS_DESC, 0L);
        cacheMissCounter = getMetricsRegistry().newCounter(
            REGIONSERVER_METADATA_CACHE_MISSES, REGIONSERVER_METADATA_CACHE_MISSES_DESC, 0L);
        validateDDLTimestampRequestCounter = getMetricsRegistry().newCounter(
            VALIDATE_LAST_DDL_TIMESTAMP_REQUESTS, VALIDATE_LAST_DDL_TIMESTAMP_REQUEST_DESC, 0L);
        cacheInvalidationOpsCounter = getMetricsRegistry().newCounter(
            METADATA_CACHE_INVALIDATION_OPERATIONS,
                METADATA_CACHE_INVALIDATION_OPERATIONS_DESC, 0L);
        cacheInvalidationSuccessCounter = getMetricsRegistry().newCounter(
            METADATA_CACHE_INVALIDATION_SUCCESS, METADATA_CACHE_INVALIDATION_SUCCESS_DESC, 0L);
        cacheInvalidationFailureCounter = getMetricsRegistry().newCounter(
            METADATA_CACHE_INVALIDATION_FAILURE, METADATA_CACHE_INVALIDATION_FAILURE_DESC, 0L);
        cacheInvalidationRpcTimeHistogram = getMetricsRegistry().newHistogram(
            METADATA_CACHE_INVALIDATION_RPC_TIME, METADATA_CACHE_INVALIDATION_RPC_TIME_DESC);
        cacheInvalidationTotalTimeHistogram = getMetricsRegistry().newHistogram(
            METADATA_CACHE_INVALIDATION_TOTAL_TIME, METADATA_CACHE_INVALIDATION_TOTAL_TIME_DESC);
    }

    @Override
    public void incrementRegionServerMetadataCacheHitCount() {
        cacheHitCounter.incr();
    }

    @Override
    public void incrementRegionServerMetadataCacheMissCount() {
        cacheMissCounter.incr();
    }

    @Override
    public void incrementValidateTimestampRequestCount() {
        validateDDLTimestampRequestCounter.incr();
    }

    @Override
    public void addMetadataCacheInvalidationRpcTime(long t) {
        cacheInvalidationRpcTimeHistogram.add(t);
    }

    @Override
    public void addMetadataCacheInvalidationTotalTime(long t) {
        cacheInvalidationTotalTimeHistogram.add(t);
    }

    @Override
    public void incrementMetadataCacheInvalidationOperationsCount() {
        cacheInvalidationOpsCounter.incr();
    }

    @Override
    public void incrementMetadataCacheInvalidationSuccessCount() {
        cacheInvalidationSuccessCounter.incr();
    }

    @Override
    public void incrementMetadataCacheInvalidationFailureCount() {
        cacheInvalidationFailureCounter.incr();
    }

    @Override
    public MetadataCachingMetricValues getCurrentMetricValues() {
        return new MetadataCachingMetricValues
                .Builder()
                .setCacheHitCount(cacheHitCounter.value())
                .setCacheMissCount(cacheMissCounter.value())
                .setValidateDDLTimestampRequestsCount(validateDDLTimestampRequestCounter.value())
                .setCacheInvalidationRpcTimeCount(cacheInvalidationRpcTimeHistogram.getCount())
                .setCacheInvalidationTotalTimeCount(cacheInvalidationTotalTimeHistogram.getCount())
                .setCacheInvalidationOpsCount(cacheInvalidationOpsCounter.value())
                .setCacheInvalidationSuccessCount(cacheInvalidationSuccessCounter.value())
                .setCacheInvalidationFailureCount(cacheInvalidationFailureCounter.value())
                .build();
    }
}
