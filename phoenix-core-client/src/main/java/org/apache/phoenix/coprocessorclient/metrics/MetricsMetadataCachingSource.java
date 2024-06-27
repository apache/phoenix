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

import org.apache.hadoop.hbase.metrics.BaseSource;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Interface for metrics about Distributed Metadata Caching
 */
public interface MetricsMetadataCachingSource extends BaseSource {
    // Metrics2 and JMX constants
    String METRICS_NAME = "MetadataCaching";
    String METRICS_CONTEXT = "phoenix";
    String METRICS_DESCRIPTION = "Metrics about Distributed Metadata Caching";
    String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

    String REGIONSERVER_METADATA_CACHE_HITS = "numRegionServerMetadataCacheHits";
    String REGIONSERVER_METADATA_CACHE_HITS_DESC
            = "Number of cache hits in PhoenixRegionServerEndpoint "
                + "when serving validate ddl timestamp requests.";

    String REGIONSERVER_METADATA_CACHE_MISSES = "numRegionServerMetadataCacheMisses";
    String REGIONSERVER_METADATA_CACHE_MISSES_DESC
            = "Number of cache misses in PhoenixRegionServerEndpoint "
                + "when serving validate ddl timestamp requests.";

    String VALIDATE_LAST_DDL_TIMESTAMP_REQUESTS = "numValidateLastDDLTimestampRequests";
    String VALIDATE_LAST_DDL_TIMESTAMP_REQUEST_DESC
            = "Number of validate last ddl timestamp requests.";

    String METADATA_CACHE_INVALIDATION_OPERATIONS = "numMetadataCacheInvalidationOps";
    String METADATA_CACHE_INVALIDATION_OPERATIONS_DESC = "Number of times we invoke "
                                                    + "cache invalidation within a DDL operation";

    String METADATA_CACHE_INVALIDATION_SUCCESS = "numMetadataCacheInvalidationOpsSuccess";
    String METADATA_CACHE_INVALIDATION_SUCCESS_DESC
            = "Number of times cache invalidation was successful.";

    String METADATA_CACHE_INVALIDATION_FAILURE = "numMetadataCacheInvalidationOpsFailure";
    String METADATA_CACHE_INVALIDATION_FAILURE_DESC = "Number of times cache invalidation failed.";

    String METADATA_CACHE_INVALIDATION_RPC_TIME = "metadataCacheInvalidationRpcTimeMs";
    String METADATA_CACHE_INVALIDATION_RPC_TIME_DESC = "Histogram for the time in milliseconds for"
                                                + " cache invalidation RPC";
    String METADATA_CACHE_INVALIDATION_TOTAL_TIME = "metadataCacheInvalidationTotalTimeMs";
    String METADATA_CACHE_INVALIDATION_TOTAL_TIME_DESC
            = "Histogram for the total time in milliseconds "
                + "for cache invalidation on all regionservers";

    /**
     * Report the number of cache hits when validating last ddl timestamps.
     */
    void incrementRegionServerMetadataCacheHitCount();

    /**
     * Report the number of cache misses when validating last ddl timestamps.
     */
    void incrementRegionServerMetadataCacheMissCount();

    /**
     * Report the number of requests for validating last ddl timestamps.
     */
    void incrementValidateTimestampRequestCount();

    /**
     * Report number of cache invalidations performed.
     */
    void incrementMetadataCacheInvalidationOperationsCount();

    /**
     * Report number of cache invalidations which were successful.
     */
    void incrementMetadataCacheInvalidationSuccessCount();

    /**
     * Report number of cache invalidations which failed.
     */
    void incrementMetadataCacheInvalidationFailureCount();

    /**
     * Add to the cache invalidation rpc time histogram.
     */
    void addMetadataCacheInvalidationRpcTime(long t);

    /**
     * Add to the cache invalidation total time histogram.
     * @param t
     */
    void addMetadataCacheInvalidationTotalTime(long t);

    /**
     * Return current values of all metrics.
     * @return {@link MetadataCachingMetricValues} object
     */
    @VisibleForTesting
    MetadataCachingMetricValues getCurrentMetricValues();

    /**
     * Class to represent values of all metrics related to server metadata caching.
     */
    @VisibleForTesting
    class MetadataCachingMetricValues {
        private long cacheHitCount;
        private long cacheMissCount;
        private long validateDDLTimestampRequestsCount;
        private long cacheInvalidationOpsCount;
        private long cacheInvalidationSuccessCount;
        private long cacheInvalidationFailureCount;
        private long cacheInvalidationRpcTimeCount;
        private long cacheInvalidationTotalTimeCount;

        MetadataCachingMetricValues(Builder builder) {
            this.cacheHitCount = builder.cacheHitCount;
            this.cacheMissCount = builder.cacheMissCount;
            this.validateDDLTimestampRequestsCount = builder.validateDDLTimestampRequestsCount;
            this.cacheInvalidationOpsCount = builder.cacheInvalidationOpsCount;
            this.cacheInvalidationSuccessCount = builder.cacheInvalidationSuccessCount;
            this.cacheInvalidationFailureCount = builder.cacheInvalidationFailureCount;
            this.cacheInvalidationRpcTimeCount = builder.cacheInvalidationRpcTimeCount;
            this.cacheInvalidationTotalTimeCount = builder.cacheInvalidationTotalTimeCount;
        }

        public long getCacheHitCount() {
            return cacheHitCount;
        }

        public long getCacheMissCount() {
            return cacheMissCount;
        }

        public long getValidateDDLTimestampRequestsCount() {
            return validateDDLTimestampRequestsCount;
        }

        public long getCacheInvalidationOpsCount() {
            return cacheInvalidationOpsCount;
        }

        public long getCacheInvalidationSuccessCount() {
            return cacheInvalidationSuccessCount;
        }

        public long getCacheInvalidationFailureCount() {
            return cacheInvalidationFailureCount;
        }

        public long getCacheInvalidationRpcTimeCount() {
            return cacheInvalidationRpcTimeCount;
        }

        public long getCacheInvalidationTotalTimeCount() {
            return cacheInvalidationTotalTimeCount;
        }

        /**
         * Builder for {@link MetadataCachingMetricValues}
         */
        public static class Builder {
            private long cacheHitCount;
            private long cacheMissCount;
            private long validateDDLTimestampRequestsCount;
            private long cacheInvalidationOpsCount;
            private long cacheInvalidationSuccessCount;
            private long cacheInvalidationFailureCount;
            private long cacheInvalidationRpcTimeCount;
            private long cacheInvalidationTotalTimeCount;

            public MetadataCachingMetricValues build() {
                return new MetadataCachingMetricValues(this);
            }

            public Builder setCacheHitCount(long c) {
                this.cacheHitCount = c;
                return this;
            }
            public Builder setCacheMissCount(long cacheMissCount) {
                this.cacheMissCount = cacheMissCount;
                return this;
            }

            public Builder setValidateDDLTimestampRequestsCount(
                    long validateDDLTimestampRequestsCount) {
                this.validateDDLTimestampRequestsCount = validateDDLTimestampRequestsCount;
                return this;
            }

            public Builder setCacheInvalidationOpsCount(long cacheInvalidationOpsCount) {
                this.cacheInvalidationOpsCount = cacheInvalidationOpsCount;
                return this;
            }

            public Builder setCacheInvalidationSuccessCount(long cacheInvalidationSuccessCount) {
                this.cacheInvalidationSuccessCount = cacheInvalidationSuccessCount;
                return this;
            }

            public Builder setCacheInvalidationFailureCount(long cacheInvalidationFailureCount) {
                this.cacheInvalidationFailureCount = cacheInvalidationFailureCount;
                return this;
            }

            public Builder setCacheInvalidationRpcTimeCount(long cacheInvalidationRpcTimeCount) {
                this.cacheInvalidationRpcTimeCount = cacheInvalidationRpcTimeCount;
                return this;
            }

            public Builder setCacheInvalidationTotalTimeCount(
                    long cacheInvalidationTotalTimeCount) {
                this.cacheInvalidationTotalTimeCount = cacheInvalidationTotalTimeCount;
                return this;
            }
        }
    }
}
