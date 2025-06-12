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
import org.apache.hadoop.metrics2.lib.MutableFastCounter;

/**
 * Implementation for tracking PhoenixMasterObserver metrics.
 */
public class MetricsPhoenixMasterSourceImpl extends BaseSourceImpl
        implements MetricsPhoenixMasterSource {

    private final MutableFastCounter postSplitPartitionUpdateFailures;
    private final MutableFastCounter postMergePartitionUpdateFailures;

    public MetricsPhoenixMasterSourceImpl() {
        this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
    }

    public MetricsPhoenixMasterSourceImpl(String metricsName, String metricsDescription,
            String metricsContext, String metricsJmxContext) {
        super(metricsName, metricsDescription, metricsContext, metricsJmxContext);

        postSplitPartitionUpdateFailures = getMetricsRegistry().newCounter(
                PHOENIX_POST_SPLIT_PARTITION_UPDATE_FAILURES,
                PHOENIX_POST_SPLIT_PARTITION_UPDATE_FAILURES_DESC, 0L);

        postMergePartitionUpdateFailures = getMetricsRegistry().newCounter(
                PHOENIX_POST_MERGE_PARTITION_UPDATE_FAILURES,
                PHOENIX_POST_MERGE_PARTITION_UPDATE_FAILURES_DESC, 0L);
    }

    @Override
    public long getPostSplitPartitionUpdateFailureCount() {
        return postSplitPartitionUpdateFailures.value();
    }

    @Override
    public void incrementPostSplitPartitionUpdateFailureCount() {
        postSplitPartitionUpdateFailures.incr();
    }

    @Override
    public long getPostMergePartitionUpdateFailureCount() {
        return postMergePartitionUpdateFailures.value();
    }

    @Override
    public void incrementPostMergePartitionUpdateFailureCount() {
        postMergePartitionUpdateFailures.incr();
    }
} 