package org.apache.phoenix.coprocessor.metrics;

import org.apache.hadoop.hbase.metrics.BaseSource;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;

public interface MetricsPhoenixTTLSource extends BaseSource {

    // Metrics2 and JMX constants
    String METRICS_NAME = "PhoenixTTLProcessor";
    String METRICS_CONTEXT = "phoenix";
    String METRICS_DESCRIPTION = "Metrics about the Phoenix TTL Coprocessor";
    String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;


    String PHOENIX_TTL_MASK_EXPIRED_REQUESTS = "phoenixMaskTTLExpiredRequests";
    String PHOENIX_TTL_MASK_EXPIRED_REQUESTS_DESC = "The number of scan requests to mask PHOENIX TTL expired rows";

    String PHOENIX_TTL_DELETE_EXPIRED_REQUESTS = "phoenixMaskTTLExpiredRequests";
    String PHOENIX_TTL_DELETE_EXPIRED_REQUESTS_DESC = "The number of delete requests to delete PHOENIX TTL expired rows";

    /**
     * Report the number of requests to mask TTL expired rows.
     */
    long getMaskExpiredRequestCount();
    /**
     * Keeps track of the number of requests to mask TTL expired rows.
     */
    void incrementMaskExpiredRequestCount();

    /**
     * Report the number of requests to mask TTL expired rows.
     */
    long getDeleteExpiredRequestCount();
    /**
     * Keeps track of the number of requests to delete TTL expired rows.
     */
    void incrementDeleteExpiredRequestCount();

}
