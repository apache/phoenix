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
package org.apache.phoenix.hbase.index;

/**
 * Observable progress of an {@link IndexCDCConsumer}.
 * <p>
 * Thread-safety: <b>single-writer.</b> The owning consumer thread is the only writer of
 * {@link #recordProcessed} / {@link #recordEmptyPoll}; the same thread also reads via
 * {@link #currentLagMs}. {@code volatile} fields provide safe publication for an off-thread
 * observer (e.g. JMX scraper) that may read {@link #getEffectiveWatermark} or
 * {@link #getLastEmptyPollEndTime}, but no off-thread <i>writer</i> is supported. Watermark is
 * monotonic; lag is clamped to zero on clock regressions.
 */
final class IndexCDCConsumerProgress {

  // Wall-clock time at consumer construction; floor for currentLagMs before any signal.
  private final long consumerStartTime;
  // Read-back buffer the consumer subtracts from `now` on its own-partition CDC filter.
  private final long timestampBufferMs;

  // Latest CDC event timestamp this consumer has acknowledged for its own partition.
  private volatile long lastProcessedTimestamp = 0L;
  // Wall-clock time captured immediately before the most recent own-partition CDC poll that
  // returned zero rows. Equals the upper bound of the poll's "no events exist below" proof:
  // (lastEmptyPollEndTime - timestampBufferMs) is the latest CDC ts the consumer is caught up to.
  private volatile long lastEmptyPollEndTime = 0L;
  // Highest own-partition CDC timestamp the consumer is confirmed caught up to (monotonic).
  private volatile long effectiveWatermark = 0L;

  IndexCDCConsumerProgress(long consumerStartTime, long timestampBufferMs) {
    this.consumerStartTime = consumerStartTime;
    this.timestampBufferMs = timestampBufferMs;
  }

  /**
   * Record progress from a successful own-partition batch. Single-writer (consumer thread only).
   */
  void recordProcessed(long ts) {
    if (ts > lastProcessedTimestamp) {
      lastProcessedTimestamp = ts;
    }
    advanceWatermark();
  }

  /** Record an own-partition CDC poll that returned zero rows. Single-writer. */
  void recordEmptyPoll(long queryStartWallClock) {
    if (queryStartWallClock > lastEmptyPollEndTime) {
      lastEmptyPollEndTime = queryStartWallClock;
    }
    advanceWatermark();
  }

  /** Current lag in milliseconds. Floors at {@code now - consumerStartTime} before any signal. */
  long currentLagMs(long now) {
    long base = effectiveWatermark > 0 ? effectiveWatermark : consumerStartTime;
    long lag = now - base;
    return lag < 0 ? 0 : lag;
  }

  private void advanceWatermark() {
    long emptyPollWatermark =
      lastEmptyPollEndTime > 0 ? lastEmptyPollEndTime - timestampBufferMs : 0L;
    long candidate = Math.max(lastProcessedTimestamp, emptyPollWatermark);
    if (candidate > effectiveWatermark) {
      effectiveWatermark = candidate;
    }
  }

  long getEffectiveWatermark() {
    return effectiveWatermark;
  }

  long getLastEmptyPollEndTime() {
    return lastEmptyPollEndTime;
  }
}
