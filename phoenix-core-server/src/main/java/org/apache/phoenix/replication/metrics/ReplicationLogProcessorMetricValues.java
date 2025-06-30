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

/** Class to hold the values of all metrics tracked by the ReplicationLogProcessor metric source. */
public class ReplicationLogProcessorMetricValues {

    private final long failedMutationsCount;
    private final long logFileReplayFailureCount;
    private final long logFileReplaySuccessCount;
    private final long logFileReplayTime;
    private final long logFileBatchReplayTime;

    public ReplicationLogProcessorMetricValues(long failedMutationsCount, 
            long logFileReplayFailureCount, long logFileReplaySuccessCount, 
            long logFileReplayTime, long logFileBatchReplayTime) {
        this.failedMutationsCount = failedMutationsCount;
        this.logFileReplayFailureCount = logFileReplayFailureCount;
        this.logFileReplaySuccessCount = logFileReplaySuccessCount;
        this.logFileReplayTime = logFileReplayTime;
        this.logFileBatchReplayTime = logFileBatchReplayTime;
    }

    public long getFailedMutationsCount() {
        return failedMutationsCount;
    }

    public long getLogFileReplayFailureCount() {
        return logFileReplayFailureCount;
    }

    public long getLogFileReplaySuccessCount() {
        return logFileReplaySuccessCount;
    }

    public long getLogFileReplayTime() {
        return logFileReplayTime;
    }

    public long getLogFileBatchReplayTime() {
        return logFileBatchReplayTime;
    }
}
