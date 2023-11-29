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
}
