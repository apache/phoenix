package org.apache.phoenix.coprocessor.metrics;

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;

public class MetricsPhoenixTTLSourceImpl extends BaseSourceImpl implements MetricsPhoenixTTLSource {
    private final MutableFastCounter maskExpiredRequests;
    private final MutableFastCounter deleteExpiredRequests;

    public MetricsPhoenixTTLSourceImpl() {
        this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
    }


    public MetricsPhoenixTTLSourceImpl(String metricsName, String metricsDescription,
            String metricsContext, String metricsJmxContext) {
        super(metricsName, metricsDescription, metricsContext, metricsJmxContext);

        maskExpiredRequests = getMetricsRegistry().newCounter(PHOENIX_TTL_MASK_EXPIRED_REQUESTS,
                PHOENIX_TTL_MASK_EXPIRED_REQUESTS_DESC, 0L);

        deleteExpiredRequests = getMetricsRegistry().newCounter(PHOENIX_TTL_DELETE_EXPIRED_REQUESTS,
                PHOENIX_TTL_DELETE_EXPIRED_REQUESTS_DESC, 0L);

    }

    @Override public void incrementMaskExpiredRequestCount() {
        maskExpiredRequests.incr();
    }

    @Override public long getMaskExpiredRequestCount() {
        return maskExpiredRequests.value();
    }

    @Override public long getDeleteExpiredRequestCount() { return deleteExpiredRequests.value(); }

    @Override public void incrementDeleteExpiredRequestCount() { deleteExpiredRequests.incr(); }
}
