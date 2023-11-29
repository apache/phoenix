package org.apache.phoenix.coprocessor.metrics;

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;

public class MetricsMetadataCachingSourceImpl
        extends BaseSourceImpl
        implements MetricsMetadataCachingSource {

    private final MutableFastCounter cacheHitCounter;
    private final MutableFastCounter cacheMissCounter;
    private final MutableFastCounter validateDDLTimestampRequestCounter;

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
}
