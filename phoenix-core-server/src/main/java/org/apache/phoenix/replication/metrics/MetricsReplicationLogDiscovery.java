/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.replication.metrics;

import org.apache.hadoop.hbase.metrics.BaseSource;

public interface MetricsReplicationLogDiscovery extends BaseSource {

    String METRICS_NAME = "MetricsReplicationLogDiscovery";
    String METRICS_CONTEXT = "phoenix";
    String METRICS_DESCRIPTION = "Metrics about Replication Log File Discovery for an HA Group";
    String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

    String NUM_ROUNDS_PROCESSED = "numRoundsProcessed";
    String NUM_ROUNDS_PROCESSED_DESC = 
        "Number of rounds processed during replication log discovery";
    String NUM_IN_PROGRESS_DIRECTORY_PROCESSED = "numInProgressDirectoryProcessed";
    String NUM_IN_PROGRESS_DIRECTORY_PROCESSED_DESC = 
        "Number of times in progress directory is processed during replication log discovery";
    String TIME_TO_PROCESS_NEW_FILES = "timeToProcessNewFilesMs";
    String TIME_TO_PROCESS_NEW_FILES_DESC = 
        "Histogram of time taken to process new files in milliseconds";
    String TIME_TO_PROCESS_IN_PROGRESS_FILES = "timeToProcessInProgressFilesMs";
    String TIME_TO_PROCESS_IN_PROGRESS_FILES_DESC = 
        "Histogram of time taken to process in progress files in milliseconds";

    /**
     * Increments the counter for rounds processed.
     * This counter tracks the number of rounds processed during replication log discovery.
     */
    void incrementNumRoundsProcessed();

    /**
     * Increments the counter for in progress directory processing.
     * This counter tracks the number of times the in progress directory is processed 
     * during replication log discovery.
     */
    void incrementNumInProgressDirectoryProcessed();

    /**
     * Update the time taken to process new files in milliseconds.
     * @param timeMs Time taken in milliseconds
     */
    void updateTimeToProcessNewFiles(long timeMs);

    /**
     * Update the time taken to process in progress files in milliseconds.
     * @param timeMs Time taken in milliseconds
     */
    void updateTimeToProcessInProgressFiles(long timeMs);

    /**
     * Unregister this metrics source.
     */
    void close();

    // Get current values for testing
    ReplicationLogFileDiscoveryMetricValues getCurrentMetricValues();

}
