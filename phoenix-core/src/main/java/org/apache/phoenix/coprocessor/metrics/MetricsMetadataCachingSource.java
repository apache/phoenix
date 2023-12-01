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

import org.apache.hadoop.hbase.metrics.BaseSource;

/**
 * Interface for metrics about Distributed Metadata Caching
 */
public interface MetricsMetadataCachingSource extends BaseSource {
    // Metrics2 and JMX constants
    String METRICS_NAME = "MetadataCaching";
    String METRICS_CONTEXT = "phoenix";
    String METRICS_DESCRIPTION = "Metrics about Distributed Metadata Caching";
    String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

    String METADATA_VALIDATION_CACHE_HIT = "metadataValidationCacheHits";
    String METADATA_VALIDATION_CACHE_HIT_DESC
            = "Number of cache hits in PhoenixRegionServerEndpoint "
                + "when serving validate ddl timestamp requests.";

    String METADATA_VALIDATION_CACHE_MISS = "metadataValidationCacheMisses";
    String METADATA_VALIDATION_CACHE_MISS_DESC
            = "Number of cache misses in PhoenixRegionServerEndpoint "
                + "when serving validate ddl timestamp requests.";

    String VALIDATE_DDL_TIMESTAMP_REQUESTS = "validateDDLTimestampRequests";
    String VALIDATE_DDL_TIMESTAMP_REQUEST_DESC = "Number of validate ddl timestamp requests.";

    String CACHE_INVALIDATION_OPERATIONS = "cacheInvalidationOperations";
    String CACHE_INVALIDATION_OPERATIONS_DESC = "Number of times we invoke cache invalidation " +
                                                    "within a DDL operation";

    String CACHE_INVALIDATION_SUCCESS = "cacheInvalidationSuccess";
    String CACHE_INVALIDATION_SUCCESS_DESC = "Number of times cache invalidation was successful.";

    String CACHE_INVALIDATION_RPC_TIME = "cacheInvalidationRpcTime";
    String CACHE_INVALIDATION_RPC_TIME_DESC = "Histogram for the time in milliseconds for "
                                                + "cache invalidation RPC";
    String CACHE_INVALIDATION_TOTAL_TIME = "cacheInvalidationTotalTime";
    String CACHE_INVALIDATION_TOTAL_TIME_DESC = "Histogram for the total time in milliseconds for "
                                                    + "cache invalidation on all regionservers";

    /**
     * Report the number of cache hits when validating last ddl timestamps.
     */
    void incrementCacheHitCount();

    /**
     * Report the number of cache misses when validating last ddl timestamps.
     */
    void incrementCacheMissCount();

    /**
     * Report the number of requests for validating last ddl timestamps.
     */
    void incrementValidateTimestampRequestCount();

    /**
     * Report number of cache invalidations performed.
     */
    void incrementCacheInvalidationOperationsCount();

    /**
     * Report number of cache invalidations which were successful.
     */
    void incrementCacheInvalidationSuccessCount();

    /**
     * Add to the cache invalidation rpc time histogram.
     */
    void addCacheInvalidationRpcTime(long t);

    /**
     * Add to the cache invalidation total time histogram.
     * @param t
     */
    void addCacheInvalidationTotalTime(long t);
}
