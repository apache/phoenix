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

/** Class to hold the values of all metrics tracked by the ReplicationLogFileTracker metrics source. */
public class ReplicationLogFileTrackerMetricValues {

    private final long markFileInProgressRequestCount;
    private final long markFileCompletedRequestCount;
    private final long markFileFailedRequestCount;
    private final long markFileCompletedRequestFailedCount;
    private final long markFileInProgressTimeMs;
    private final long markFileCompletedTimeMs;
    private final long markFileFailedTimeMs;

    public ReplicationLogFileTrackerMetricValues(long markFileInProgressRequestCount, long markFileCompletedRequestCount,
        long markFileFailedRequestCount, long markFileCompletedRequestFailedCount,
        long markFileInProgressTimeMs, long markFileCompletedTimeMs, long markFileFailedTimeMs) {
        this.markFileInProgressRequestCount = markFileInProgressRequestCount;
        this.markFileCompletedRequestCount = markFileCompletedRequestCount;
        this.markFileFailedRequestCount = markFileFailedRequestCount;
        this.markFileCompletedRequestFailedCount = markFileCompletedRequestFailedCount;
        this.markFileInProgressTimeMs = markFileInProgressTimeMs;
        this.markFileCompletedTimeMs = markFileCompletedTimeMs;
        this.markFileFailedTimeMs = markFileFailedTimeMs;
    }

    public long getMarkFileInProgressRequestCount() {
        return markFileInProgressRequestCount;
    }

    public long getMarkFileCompletedRequestCount() {
        return markFileCompletedRequestCount;
    }

    public long getMarkFileFailedRequestCount() {
        return markFileFailedRequestCount;
    }

    public long getMarkFileCompletedRequestFailedCount() {
        return markFileCompletedRequestFailedCount;
    }

    public long getMarkFileInProgressTimeMs() {
        return markFileInProgressTimeMs;
    }

    public long getMarkFileCompletedTimeMs() {
        return markFileCompletedTimeMs;
    }

    public long getMarkFileFailedTimeMs() {
        return markFileFailedTimeMs;
    }

}
