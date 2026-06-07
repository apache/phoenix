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

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.hbase.metrics.Snapshot;
import org.apache.hadoop.hbase.metrics.impl.HistogramImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.hadoop.metrics2.lib.MutableHistogram;
import org.apache.hadoop.metrics2.lib.MutableSizeHistogram;
import org.apache.hadoop.metrics2.lib.MutableTimeHistogram;

/** Implementation of metrics source for ReplicationLog operations. */
public class MetricsReplicationLogGroupSourceImpl extends BaseSourceImpl
  implements MetricsReplicationLogGroupSource {

  private final MutableFastCounter rotationCount;
  private final MutableFastCounter rotationFailuresCount;
  private final MutableFastCounter syncToSafTransitions;
  private final MutableHistogram appendTimeNs;
  private final MutableTimeHistogram syncTimeMs;
  private final MutableTimeHistogram rotationTimeMs;
  private final MutableHistogram ringBufferTimeNs;
  private final MutableTimeHistogram fsSyncTimeMs;
  private final MutableSizeHistogram batchSize;
  private final MutableSizeHistogram pendingSyncCount;
  private final MutableHistogram pendingSyncWaitTimeNs;

  public MetricsReplicationLogGroupSourceImpl(String haGroupName) {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT, haGroupName);
  }

  public MetricsReplicationLogGroupSourceImpl(String metricsName, String metricsDescription,
    String metricsContext, String metricsJmxContext, String haGroupName) {
    super(metricsName, metricsDescription, metricsContext,
      metricsJmxContext + ",haGroup=" + haGroupName);
    rotationCount = getMetricsRegistry().newCounter(ROTATION_COUNT, ROTATION_COUNT_DESC, 0L);
    rotationFailuresCount =
      getMetricsRegistry().newCounter(ROTATION_FAILURES, ROTATION_FAILURES_DESC, 0L);
    syncToSafTransitions =
      getMetricsRegistry().newCounter(SYNC_TO_SAF_TRANSITIONS, SYNC_TO_SAF_TRANSITIONS_DESC, 0L);
    appendTimeNs = getMetricsRegistry().newHistogram(APPEND_TIME, APPEND_TIME_DESC);
    syncTimeMs = getMetricsRegistry().newTimeHistogram(SYNC_TIME, SYNC_TIME_DESC);
    rotationTimeMs = getMetricsRegistry().newTimeHistogram(ROTATION_TIME, ROTATION_TIME_DESC);
    ringBufferTimeNs = getMetricsRegistry().newHistogram(RING_BUFFER_TIME, RING_BUFFER_TIME_DESC);
    fsSyncTimeMs = getMetricsRegistry().newTimeHistogram(FS_SYNC_TIME, FS_SYNC_TIME_DESC);
    batchSize = getMetricsRegistry().newSizeHistogram(BATCH_SIZE, BATCH_SIZE_DESC);
    pendingSyncCount =
      getMetricsRegistry().newSizeHistogram(PENDING_SYNC_COUNT, PENDING_SYNC_COUNT_DESC);
    pendingSyncWaitTimeNs =
      getMetricsRegistry().newHistogram(PENDING_SYNC_WAIT_TIME, PENDING_SYNC_WAIT_TIME_DESC);
  }

  @Override
  public void close() {
    DefaultMetricsSystem.instance().unregisterSource(metricsJmxContext);
  }

  @Override
  public void incrementRotationCount() {
    rotationCount.incr();
  }

  @Override
  public void incrementRotationFailureCount() {
    rotationFailuresCount.incr();
  }

  @Override
  public void incrementSyncToSafTransitions() {
    syncToSafTransitions.incr();
  }

  @Override
  public void updateAppendTime(long timeNs) {
    appendTimeNs.add(timeNs);
  }

  @Override
  public void updateSyncTime(long timeNs) {
    syncTimeMs.add(TimeUnit.NANOSECONDS.toMillis(timeNs));
  }

  @Override
  public void updateRotationTime(long timeNs) {
    rotationTimeMs.add(TimeUnit.NANOSECONDS.toMillis(timeNs));
  }

  @Override
  public void updateRingBufferTime(long timeNs) {
    ringBufferTimeNs.add(timeNs);
  }

  @Override
  public void updateFsSyncTime(long timeNs) {
    fsSyncTimeMs.add(TimeUnit.NANOSECONDS.toMillis(timeNs));
  }

  @Override
  public void updateBatchSize(long size) {
    batchSize.add(size);
  }

  @Override
  public void updatePendingSyncCount(long count) {
    pendingSyncCount.add(count);
  }

  @Override
  public void updatePendingSyncWaitTime(long timeNs) {
    pendingSyncWaitTimeNs.add(timeNs);
  }

  /**
   * Test-facing accessor that snapshots the four time histograms used in the producer-side sync
   * decomposition (sync, ringBuffer, fsSync, pendingSyncWait) and returns max + p50 + p99 for each.
   * <p>
   * <b>Side effect:</b> the snapshot resets the underlying FastLongHistogram bins, so this method
   * is destructive and intended for end-of-test inspection only. Subsequent histogram reads will
   * see only data added after this call.
   */
  @Override
  public ReplicationLogMetricValues getCurrentMetricValues() {
    Snapshot syncSnap = snapshot(syncTimeMs);
    Snapshot ringSnap = snapshot(ringBufferTimeNs);
    Snapshot fsSnap = snapshot(fsSyncTimeMs);
    Snapshot pendSnap = snapshot(pendingSyncWaitTimeNs);
    return ReplicationLogMetricValues.builder().rotationCount(rotationCount.value())
      .rotationFailuresCount(rotationFailuresCount.value())
      .syncToSafTransitions(syncToSafTransitions.value()).appendTimeMax(appendTimeNs.getMax())
      .syncTimeMax(syncSnap.getMax()).syncTimeP50(syncSnap.getMedian())
      .syncTimeP99(syncSnap.get99thPercentile()).rotationTimeMax(rotationTimeMs.getMax())
      .ringBufferTimeMax(ringSnap.getMax()).ringBufferTimeP50(ringSnap.getMedian())
      .ringBufferTimeP99(ringSnap.get99thPercentile()).fsSyncTimeMax(fsSnap.getMax())
      .fsSyncTimeP50(fsSnap.getMedian()).fsSyncTimeP99(fsSnap.get99thPercentile())
      .batchSizeMax(batchSize.getMax()).pendingSyncCountMax(pendingSyncCount.getMax())
      .pendingSyncWaitTimeMax(pendSnap.getMax()).pendingSyncWaitTimeP50(pendSnap.getMedian())
      .pendingSyncWaitTimeP99(pendSnap.get99thPercentile()).build();
  }

  /**
   * Reach into MutableHistogram via reflection to call HistogramImpl.snapshot(), which exposes the
   * full percentile distribution. The protected {@code MutableHistogram.histogram} field is the
   * only path to these percentiles short of subclassing every histogram type.
   */
  private static Snapshot snapshot(MutableHistogram histogram) {
    try {
      Field field = MutableHistogram.class.getDeclaredField("histogram");
      field.setAccessible(true);
      HistogramImpl impl = (HistogramImpl) field.get(histogram);
      return impl.snapshot();
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Failed to access MutableHistogram.histogram via reflection",
        e);
    }
  }

  @Override
  public String getMetricsName() {
    return METRICS_NAME;
  }

  @Override
  public String getMetricsDescription() {
    return METRICS_DESCRIPTION;
  }

  @Override
  public String getMetricsContext() {
    return METRICS_CONTEXT;
  }
}
