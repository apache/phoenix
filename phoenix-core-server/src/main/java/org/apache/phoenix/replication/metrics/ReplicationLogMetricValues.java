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

/** Class to hold the values of all metrics tracked by the ReplicationLog metrics source. */
public class ReplicationLogMetricValues {

  private final long rotationCount;
  private final long rotationFailuresCount;
  private final long syncToSafTransitions;
  private final long appendTime;
  private final long syncTime;
  private final long rotationTime;
  private final long ringBufferTime;
  private final long fsSyncTime;
  private final long batchSize;
  private final long pendingSyncCount;
  private final long pendingSyncWaitTime;

  private ReplicationLogMetricValues(Builder b) {
    this.rotationCount = b.rotationCount;
    this.rotationFailuresCount = b.rotationFailuresCount;
    this.syncToSafTransitions = b.syncToSafTransitions;
    this.appendTime = b.appendTime;
    this.syncTime = b.syncTime;
    this.rotationTime = b.rotationTime;
    this.ringBufferTime = b.ringBufferTime;
    this.fsSyncTime = b.fsSyncTime;
    this.batchSize = b.batchSize;
    this.pendingSyncCount = b.pendingSyncCount;
    this.pendingSyncWaitTime = b.pendingSyncWaitTime;
  }

  public static Builder builder() {
    return new Builder();
  }

  public long getRotationCount() {
    return rotationCount;
  }

  public long getRotationFailuresCount() {
    return rotationFailuresCount;
  }

  public long getSyncToSafTransitions() {
    return syncToSafTransitions;
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

  public long getFsSyncTime() {
    return fsSyncTime;
  }

  public long getBatchSize() {
    return batchSize;
  }

  public long getPendingSyncCount() {
    return pendingSyncCount;
  }

  public long getPendingSyncWaitTime() {
    return pendingSyncWaitTime;
  }

  public static class Builder {
    private long rotationCount;
    private long rotationFailuresCount;
    private long syncToSafTransitions;
    private long appendTime;
    private long syncTime;
    private long rotationTime;
    private long ringBufferTime;
    private long fsSyncTime;
    private long batchSize;
    private long pendingSyncCount;
    private long pendingSyncWaitTime;

    public Builder rotationCount(long v) {
      this.rotationCount = v;
      return this;
    }

    public Builder rotationFailuresCount(long v) {
      this.rotationFailuresCount = v;
      return this;
    }

    public Builder syncToSafTransitions(long v) {
      this.syncToSafTransitions = v;
      return this;
    }

    public Builder appendTime(long v) {
      this.appendTime = v;
      return this;
    }

    public Builder syncTime(long v) {
      this.syncTime = v;
      return this;
    }

    public Builder rotationTime(long v) {
      this.rotationTime = v;
      return this;
    }

    public Builder ringBufferTime(long v) {
      this.ringBufferTime = v;
      return this;
    }

    public Builder fsSyncTime(long v) {
      this.fsSyncTime = v;
      return this;
    }

    public Builder batchSize(long v) {
      this.batchSize = v;
      return this;
    }

    public Builder pendingSyncCount(long v) {
      this.pendingSyncCount = v;
      return this;
    }

    public Builder pendingSyncWaitTime(long v) {
      this.pendingSyncWaitTime = v;
      return this;
    }

    public ReplicationLogMetricValues build() {
      return new ReplicationLogMetricValues(this);
    }
  }
}
