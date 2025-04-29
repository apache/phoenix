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

/** Class to hold the current values of all ReplicationLog metrics. */
public class ReplicationLogMetricValues {

    private final long timeBasedRotationCount;
    private final long sizeBasedRotationCount;
    private final long errorBasedRotationCount;
    private final long totalRotationCount;
    private final long appendTime;
    private final long syncTime;
    private final long rotationTime;
    private final long ringBufferTime;


    private ReplicationLogMetricValues(Builder builder) {
        this.timeBasedRotationCount = builder.timeBasedRotationCounter;
        this.sizeBasedRotationCount = builder.sizeBasedRotationCounter;
        this.errorBasedRotationCount = builder.errorBasedRotationCounter;
        this.totalRotationCount = builder.totalRotationCounter;
        this.appendTime = builder.appendTimeCount;
        this.syncTime = builder.syncTimeCount;
        this.rotationTime = builder.rotationTimeCount;
        this.ringBufferTime = builder.ringBufferTimeCount;
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

    public long getTotalRotationCount() {
        return totalRotationCount;
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

    public static class Builder {
        private long timeBasedRotationCounter;
        private long sizeBasedRotationCounter;
        private long errorBasedRotationCounter;
        private long totalRotationCounter;
        private long appendTimeCount;
        private long syncTimeCount;
        private long rotationTimeCount;
        private long ringBufferTimeCount;

        public Builder setTimeBasedRotationCount(long timeBasedRotationCounter) {
            this.timeBasedRotationCounter = timeBasedRotationCounter;
            return this;
        }

        public Builder setSizeBasedRotationCount(long sizeBasedRotationCounter) {
            this.sizeBasedRotationCounter = sizeBasedRotationCounter;
            return this;
        }

        public Builder setErrorBasedRotationCount(long errorBasedRotationCounter) {
            this.errorBasedRotationCounter = errorBasedRotationCounter;
            return this;
        }

        public Builder setTotalRotationCount(long totalRotationCounter) {
            this.totalRotationCounter = totalRotationCounter;
            return this;
        }

        public Builder setAppendTime(long appendTimeCount) {
            this.appendTimeCount = appendTimeCount;
            return this;
        }

        public Builder setSyncTime(long syncTimeCount) {
            this.syncTimeCount = syncTimeCount;
            return this;
        }

        public Builder setRotationTime(long rotationTimeCount) {
            this.rotationTimeCount = rotationTimeCount;
            return this;
        }

        public Builder setRingBufferTime(long ringBufferTimeCount) {
            this.ringBufferTimeCount = ringBufferTimeCount;
            return this;
        }

        public ReplicationLogMetricValues build() {
            return new ReplicationLogMetricValues(this);
        }
    }

}
