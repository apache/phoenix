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
package org.apache.phoenix.coprocessor.metrics;

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
    private final MutableFastCounter getCacheInvalidationSuccessCounter;
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
                METADATA_VALIDATION_CACHE_HIT, METADATA_VALIDATION_CACHE_HIT_DESC, 0L);
        cacheMissCounter = getMetricsRegistry().newCounter(
                METADATA_VALIDATION_CACHE_MISS, METADATA_VALIDATION_CACHE_MISS_DESC, 0L);
        validateDDLTimestampRequestCounter = getMetricsRegistry().newCounter(
                VALIDATE_DDL_TIMESTAMP_REQUESTS, VALIDATE_DDL_TIMESTAMP_REQUEST_DESC, 0L);
        cacheInvalidationOpsCounter = getMetricsRegistry().newCounter(
                CACHE_INVALIDATION_OPERATIONS, CACHE_INVALIDATION_OPERATIONS_DESC, 0L);
        getCacheInvalidationSuccessCounter = getMetricsRegistry().newCounter(
                CACHE_INVALIDATION_SUCCESS, CACHE_INVALIDATION_SUCCESS_DESC, 0L);
        cacheInvalidationRpcTimeHistogram = getMetricsRegistry().newHistogram(
                CACHE_INVALIDATION_RPC_TIME, CACHE_INVALIDATION_RPC_TIME_DESC);
        cacheInvalidationTotalTimeHistogram = getMetricsRegistry().newHistogram(
                CACHE_INVALIDATION_TOTAL_TIME, CACHE_INVALIDATION_TOTAL_TIME_DESC);
    }

    @Override
    public void incrementCacheHitCount() {
        cacheHitCounter.incr();
    }

    @Override
    public void incrementCacheMissCount() {
        cacheMissCounter.incr();
    }

    @Override
    public void incrementValidateTimestampRequestCount() {
        validateDDLTimestampRequestCounter.incr();
    }

    @Override
    public void addCacheInvalidationRpcTime(long t) {
        cacheInvalidationRpcTimeHistogram.add(t);
    }

    @Override
    public void addCacheInvalidationTotalTime(long t) {
        cacheInvalidationTotalTimeHistogram.add(t);
    }

    @Override
    public void incrementCacheInvalidationOperationsCount() {
        cacheInvalidationOpsCounter.incr();
    }

    @Override
    public void incrementCacheInvalidationSuccessCount() {
        getCacheInvalidationSuccessCounter.incr();
    }
}
