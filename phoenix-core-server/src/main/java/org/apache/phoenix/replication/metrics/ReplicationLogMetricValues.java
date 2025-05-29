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
package org.apache.phoenix.replication.metrics;

/** Class to hold the values of all metrics tracked by the ReplicationLog metrics source. */
public class ReplicationLogMetricValues {

    private final long timeBasedRotationCount;
    private final long sizeBasedRotationCount;
    private final long errorBasedRotationCount;
    private final long rotationCount;
    private final long rotationFailuresCount;
    private final long appendTime;
    private final long syncTime;
    private final long rotationTime;
    private final long ringBufferTime;

    public ReplicationLogMetricValues(long timeBasedRotationCount, long sizeBasedRotationCount,
        long errorBasedRotationCount, long rotationCount, long rotationFailuresCount,
        long appendTime, long syncTime, long rotationTime, long ringBufferTime) {
        this.timeBasedRotationCount = timeBasedRotationCount;
        this.sizeBasedRotationCount = sizeBasedRotationCount;
        this.errorBasedRotationCount = errorBasedRotationCount;
        this.rotationCount = rotationCount;
        this.rotationFailuresCount = rotationFailuresCount;
        this.appendTime = appendTime;
        this.syncTime = syncTime;
        this.rotationTime = rotationTime;
        this.ringBufferTime = ringBufferTime;
    }

    public long getTimeBasedRotationCount() {
        return timeBasedRotationCount;
    }

    public long getSizeBasedRotationCount() {
        return sizeBasedRotationCount;
    }

    public long getErrorBasedRotationCount() {
        return errorBasedRotationCount;
    }

    public long getRotationCount() {
        return rotationCount;
    }

    public long getRotationFailuresCount() {
        return rotationFailuresCount;
    }

    public long getAppendTime() {
        return appendTime;
    }

    public long getSyncTime() {
        return syncTime;
    }

    public long getRotationTime() {
        return rotationTime;
    }

    public long getRingBufferTime() {
        return ringBufferTime;
    }

}
