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
import org.apache.hadoop.metrics2.lib.MutableFastCounter;

/**
 * Implementation for tracking {@link org.apache.phoenix.coprocessor.PhoenixTTLRegionObserver} metrics.
 */
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
