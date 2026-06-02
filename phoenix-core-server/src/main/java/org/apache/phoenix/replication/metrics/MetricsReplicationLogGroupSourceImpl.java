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

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.hadoop.metrics2.lib.MutableHistogram;

/** Implementation of metrics source for ReplicationLog operations. */
public class MetricsReplicationLogGroupSourceImpl extends BaseSourceImpl
  implements MetricsReplicationLogGroupSource {

  private final MutableFastCounter rotationCount;
  private final MutableFastCounter rotationFailuresCount;
  private final MutableFastCounter syncToSafTransitions;
  private final MutableHistogram appendTime;
  private final MutableHistogram syncTime;
  private final MutableHistogram rotationTime;
  private final MutableHistogram ringBufferTime;
  private final MutableHistogram fsSyncTime;
  private final MutableHistogram batchSize;
  private final MutableHistogram pendingSyncCount;
  private final MutableHistogram pendingSyncWaitTime;

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
    appendTime = getMetricsRegistry().newHistogram(APPEND_TIME, APPEND_TIME_DESC);
    syncTime = getMetricsRegistry().newHistogram(SYNC_TIME, SYNC_TIME_DESC);
    rotationTime = getMetricsRegistry().newHistogram(ROTATION_TIME, ROTATION_TIME_DESC);
    ringBufferTime = getMetricsRegistry().newHistogram(RING_BUFFER_TIME, RING_BUFFER_TIME_DESC);
    fsSyncTime = getMetricsRegistry().newHistogram(FS_SYNC_TIME, FS_SYNC_TIME_DESC);
    batchSize = getMetricsRegistry().newHistogram(BATCH_SIZE, BATCH_SIZE_DESC);
    pendingSyncCount =
      getMetricsRegistry().newHistogram(PENDING_SYNC_COUNT, PENDING_SYNC_COUNT_DESC);
    pendingSyncWaitTime =
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
    appendTime.add(timeNs);
  }

  @Override
  public void updateSyncTime(long timeNs) {
    syncTime.add(timeNs);
  }

  @Override
  public void updateRotationTime(long timeNs) {
    rotationTime.add(timeNs);
  }

  @Override
  public void updateRingBufferTime(long timeNs) {
    ringBufferTime.add(timeNs);
  }

  @Override
  public void updateFsSyncTime(long timeNs) {
    fsSyncTime.add(timeNs);
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
    pendingSyncWaitTime.add(timeNs);
  }

  @Override
  public ReplicationLogMetricValues getCurrentMetricValues() {
    return ReplicationLogMetricValues.builder().rotationCount(rotationCount.value())
      .rotationFailuresCount(rotationFailuresCount.value())
      .syncToSafTransitions(syncToSafTransitions.value()).appendTime(appendTime.getMax())
      .syncTime(syncTime.getMax()).rotationTime(rotationTime.getMax())
      .ringBufferTime(ringBufferTime.getMax()).fsSyncTime(fsSyncTime.getMax())
      .batchSize(batchSize.getMax()).pendingSyncCount(pendingSyncCount.getMax())
      .pendingSyncWaitTime(pendingSyncWaitTime.getMax()).build();
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
