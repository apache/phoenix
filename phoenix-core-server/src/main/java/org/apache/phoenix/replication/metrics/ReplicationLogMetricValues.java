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

/**
 * Class to hold the values of all metrics tracked by the ReplicationLog metrics source. The
 * time-named getters (e.g. {@link #getSyncTime()}) return the max observed value. Percentile
 * accessors (e.g. {@link #getSyncTimeP50()}, {@link #getSyncTimeP99()}) are populated when the
 * source produces this DTO via a destructive snapshot path; otherwise they are zero.
 */
public class ReplicationLogMetricValues {

  private final long rotationCount;
  private final long rotationFailuresCount;
  private final long syncToSafTransitions;
  private final long appendTimeNs;
  private final long syncTimeMs;
  private final long syncTimeP50Ms;
  private final long syncTimeP99Ms;
  private final long rotationTimeMs;
  private final long ringBufferTimeNs;
  private final long ringBufferTimeP50Ns;
  private final long ringBufferTimeP99Ns;
  private final long fsSyncTimeMs;
  private final long fsSyncTimeP50Ms;
  private final long fsSyncTimeP99Ms;
  private final long batchSize;
  private final long pendingSyncCount;
  private final long pendingSyncWaitTimeNs;
  private final long pendingSyncWaitTimeP50Ns;
  private final long pendingSyncWaitTimeP99Ns;

  private ReplicationLogMetricValues(Builder b) {
    this.rotationCount = b.rotationCount;
    this.rotationFailuresCount = b.rotationFailuresCount;
    this.syncToSafTransitions = b.syncToSafTransitions;
    this.appendTimeNs = b.appendTimeNs;
    this.syncTimeMs = b.syncTimeMs;
    this.syncTimeP50Ms = b.syncTimeP50Ms;
    this.syncTimeP99Ms = b.syncTimeP99Ms;
    this.rotationTimeMs = b.rotationTimeMs;
    this.ringBufferTimeNs = b.ringBufferTimeNs;
    this.ringBufferTimeP50Ns = b.ringBufferTimeP50Ns;
    this.ringBufferTimeP99Ns = b.ringBufferTimeP99Ns;
    this.fsSyncTimeMs = b.fsSyncTimeMs;
    this.fsSyncTimeP50Ms = b.fsSyncTimeP50Ms;
    this.fsSyncTimeP99Ms = b.fsSyncTimeP99Ms;
    this.batchSize = b.batchSize;
    this.pendingSyncCount = b.pendingSyncCount;
    this.pendingSyncWaitTimeNs = b.pendingSyncWaitTimeNs;
    this.pendingSyncWaitTimeP50Ns = b.pendingSyncWaitTimeP50Ns;
    this.pendingSyncWaitTimeP99Ns = b.pendingSyncWaitTimeP99Ns;
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

  public long getAppendTimeMax() {
    return appendTimeNs;
  }

  public long getSyncTimeMax() {
    return syncTimeMs;
  }

  public long getSyncTimeP50() {
    return syncTimeP50Ms;
  }

  public long getSyncTimeP99() {
    return syncTimeP99Ms;
  }

  public long getRotationTimeMax() {
    return rotationTimeMs;
  }

  public long getRingBufferTimeMax() {
    return ringBufferTimeNs;
  }

  public long getRingBufferTimeP50() {
    return ringBufferTimeP50Ns;
  }

  public long getRingBufferTimeP99() {
    return ringBufferTimeP99Ns;
  }

  public long getFsSyncTimeMax() {
    return fsSyncTimeMs;
  }

  public long getFsSyncTimeP50() {
    return fsSyncTimeP50Ms;
  }

  public long getFsSyncTimeP99() {
    return fsSyncTimeP99Ms;
  }

  public long getBatchSizeMax() {
    return batchSize;
  }

  public long getPendingSyncCountMax() {
    return pendingSyncCount;
  }

  public long getPendingSyncWaitTimeMax() {
    return pendingSyncWaitTimeNs;
  }

  public long getPendingSyncWaitTimeP50() {
    return pendingSyncWaitTimeP50Ns;
  }

  public long getPendingSyncWaitTimeP99() {
    return pendingSyncWaitTimeP99Ns;
  }

  public static class Builder {
    private long rotationCount;
    private long rotationFailuresCount;
    private long syncToSafTransitions;
    private long appendTimeNs;
    private long syncTimeMs;
    private long syncTimeP50Ms;
    private long syncTimeP99Ms;
    private long rotationTimeMs;
    private long ringBufferTimeNs;
    private long ringBufferTimeP50Ns;
    private long ringBufferTimeP99Ns;
    private long fsSyncTimeMs;
    private long fsSyncTimeP50Ms;
    private long fsSyncTimeP99Ms;
    private long batchSize;
    private long pendingSyncCount;
    private long pendingSyncWaitTimeNs;
    private long pendingSyncWaitTimeP50Ns;
    private long pendingSyncWaitTimeP99Ns;

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

    public Builder appendTimeMax(long v) {
      this.appendTimeNs = v;
      return this;
    }

    public Builder syncTimeMax(long v) {
      this.syncTimeMs = v;
      return this;
    }

    public Builder syncTimeP50(long v) {
      this.syncTimeP50Ms = v;
      return this;
    }

    public Builder syncTimeP99(long v) {
      this.syncTimeP99Ms = v;
      return this;
    }

    public Builder rotationTimeMax(long v) {
      this.rotationTimeMs = v;
      return this;
    }

    public Builder ringBufferTimeMax(long v) {
      this.ringBufferTimeNs = v;
      return this;
    }

    public Builder ringBufferTimeP50(long v) {
      this.ringBufferTimeP50Ns = v;
      return this;
    }

    public Builder ringBufferTimeP99(long v) {
      this.ringBufferTimeP99Ns = v;
      return this;
    }

    public Builder fsSyncTimeMax(long v) {
      this.fsSyncTimeMs = v;
      return this;
    }

    public Builder fsSyncTimeP50(long v) {
      this.fsSyncTimeP50Ms = v;
      return this;
    }

    public Builder fsSyncTimeP99(long v) {
      this.fsSyncTimeP99Ms = v;
      return this;
    }

    public Builder batchSizeMax(long v) {
      this.batchSize = v;
      return this;
    }

    public Builder pendingSyncCountMax(long v) {
      this.pendingSyncCount = v;
      return this;
    }

    public Builder pendingSyncWaitTimeMax(long v) {
      this.pendingSyncWaitTimeNs = v;
      return this;
    }

    public Builder pendingSyncWaitTimeP50(long v) {
      this.pendingSyncWaitTimeP50Ns = v;
      return this;
    }

    public Builder pendingSyncWaitTimeP99(long v) {
      this.pendingSyncWaitTimeP99Ns = v;
      return this;
    }

    public ReplicationLogMetricValues build() {
      return new ReplicationLogMetricValues(this);
    }
  }
}
