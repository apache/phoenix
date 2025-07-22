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
 * PhoenixMasterObserver metrics source.
 */
public interface MetricsPhoenixMasterSource extends BaseSource {

    String METRICS_NAME = "PhoenixMasterObserver";
    String METRICS_CONTEXT = "phoenix";
    String METRICS_DESCRIPTION = "Metrics about the Phoenix Master Coprocessor";
    String METRICS_JMX_CONTEXT = "Master,sub=" + METRICS_NAME;

    String PHOENIX_POST_SPLIT_PARTITION_UPDATE_FAILURES = "phoenixPostSplitPartitionUpdateFailures";
    String PHOENIX_POST_SPLIT_PARTITION_UPDATE_FAILURES_DESC =
            "The number of failures during partition metadata updates after region splits";

    String PHOENIX_POST_MERGE_PARTITION_UPDATE_FAILURES = "phoenixPostMergePartitionUpdateFailures";
    String PHOENIX_POST_MERGE_PARTITION_UPDATE_FAILURES_DESC =
            "The number of failures during partition metadata updates after region merges";

    /**
     * Return the number of failures during partition metadata updates after region splits.
     */
    long getPostSplitPartitionUpdateFailureCount();

    /**
     * Increment the number of failures during partition metadata updates after region splits.
     */
    void incrementPostSplitPartitionUpdateFailureCount();

    /**
     * Return the number of failures during partition metadata updates after region merges.
     */
    long getPostMergePartitionUpdateFailureCount();

    /**
     * Increment the number of failures during partition metadata updates after region merges.
     */
    void incrementPostMergePartitionUpdateFailureCount();
} 