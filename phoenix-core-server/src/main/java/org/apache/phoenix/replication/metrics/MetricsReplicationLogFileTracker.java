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

public interface MetricsReplicationLogFileTracker extends BaseSource {

    String METRICS_NAME = "MetricsReplicationLogFileTracker";
    String METRICS_CONTEXT = "phoenix";
    String METRICS_DESCRIPTION = "Metrics about Replication Log File Tracker for an HA Group";
    String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

    String MARK_FILE_IN_PROGRESS_REQUEST_COUNT = "markFileInProgressRequestCount";
    String MARK_FILE_IN_PROGRESS_REQUEST_COUNT_DESC = 
        "Number of requests made to mark file in progress";
    String MARK_FILE_COMPLETED_REQUEST_COUNT = "markFileCompletedRequestCount";
    String MARK_FILE_COMPLETED_REQUEST_COUNT_DESC = 
        "Number of requests made to mark file completed";
    String MARK_FILE_FAILED_REQUEST_COUNT = "markFileFailedRequestCount";
    String MARK_FILE_FAILED_REQUEST_COUNT_DESC = "Number of requests made to mark file failed";
    String MARK_FILE_COMPLETED_REQUEST_FAILED_COUNT = "markFileCompletedRequestFailedCount";
    String MARK_FILE_COMPLETED_REQUEST_FAILED_COUNT_DESC = 
        "Number of requests made to mark file completed failed";
    String MARK_FILE_IN_PROGRESS_TIME = "markFileInProgressTimeMs";
    String MARK_FILE_IN_PROGRESS_TIME_DESC = 
        "Histogram of time taken for marking a file in progress in milliseconds";
    String MARK_FILE_COMPLETED_TIME = "markFileCompletedTimeMs";
    String MARK_FILE_COMPLETED_TIME_DESC = 
        "Histogram of time taken for marking a file completed in milliseconds";
    String MARK_FILE_FAILED_TIME = "markFileFailedTimeMs";
    String MARK_FILE_FAILED_TIME_DESC = 
        "Histogram of time taken for marking a file failed in milliseconds";

    /**
     * Increments the counter for mark file in progress requests.
     * This counter tracks the number of requests made to mark files in progress.
     */
    void incrementMarkFileInProgressRequestCount();

    /**
     * Increments the counter for mark file completed requests.
     * This counter tracks the number of requests made to mark files completed.
     */
    void incrementMarkFileCompletedRequestCount();

    /**
     * Increments the counter for mark file failed requests.
     * This counter tracks the number of requests made to mark files failed.
     */
    void incrementMarkFileFailedRequestCount();

    /**
     * Increments the counter for mark file completed request failures.
     * This counter tracks the number of requests made to mark files completed that failed.
     */
    void incrementMarkFileCompletedRequestFailedCount();

    /**
     * Update the time taken for marking a file in progress in milliseconds.
     * @param timeMs Time taken in milliseconds
     */
    void updateMarkFileInProgressTime(long timeMs);

    /**
     * Update the time taken for marking a file completed in milliseconds.
     * @param timeMs Time taken in milliseconds
     */
    void updateMarkFileCompletedTime(long timeMs);

    /**
     * Update the time taken for marking a file failed in milliseconds.
     * @param timeMs Time taken in milliseconds
     */
    void updateMarkFileFailedTime(long timeMs);

    /**
     * Unregister this metrics source.
     */
    void close();

    // Get current values for testing
    ReplicationLogFileTrackerMetricValues getCurrentMetricValues();

}
