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

/**
 * Interface for metrics about {@link org.apache.phoenix.coprocessor.PhoenixTTLRegionObserver}.
 */
public interface MetricsPhoenixTTLSource extends BaseSource {

    // Metrics2 and JMX constants
    String METRICS_NAME = "PhoenixTTLProcessor";
    String METRICS_CONTEXT = "phoenix";
    String METRICS_DESCRIPTION = "Metrics about the Phoenix TTL Coprocessor";
    String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;


    String PHOENIX_TTL_MASK_EXPIRED_REQUESTS = "phoenixMaskTTLExpiredRequests";
    String PHOENIX_TTL_MASK_EXPIRED_REQUESTS_DESC = "The number of scan requests to mask PHOENIX TTL expired rows";

    String PHOENIX_TTL_DELETE_EXPIRED_REQUESTS = "phoenixDeleteTTLExpiredRequests";
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
