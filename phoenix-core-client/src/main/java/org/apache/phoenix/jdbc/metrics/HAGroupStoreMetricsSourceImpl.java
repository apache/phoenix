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
package org.apache.phoenix.jdbc.metrics;

import java.util.concurrent.TimeUnit;
import javax.management.ObjectName;
import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableTimeHistogram;
import org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;

/** Hadoop Metrics2 implementation for one HAGroupStore group. */
public class HAGroupStoreMetricsSourceImpl extends BaseSourceImpl
  implements HAGroupStoreMetricsSource {

  private final MutableGaugeLong localCacheHealthStatus;
  private final MutableGaugeLong peerVisibilityStatus;
  private final MutableGaugeLong degradedStandbyActive;
  private final MutableGaugeLong currentLocalState;
  private final MutableGaugeLong currentPeerState;

  private final MutableFastCounter localZkConnectionLostCount;
  private final MutableFastCounter peerBlindCount;
  private final MutableFastCounter degradedStandbyPresentedCount;
  private final MutableFastCounter invalidTransitionRejectedCount;
  private final MutableFastCounter systemTableSyncFailedCount;
  private final MutableFastCounter notificationListenerErrorCount;

  private final MutableTimeHistogram subscriberNotifyTimeMs;

  public HAGroupStoreMetricsSourceImpl(String haGroupName) {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT, haGroupName);
  }

  HAGroupStoreMetricsSourceImpl(String metricsName, String metricsDescription,
    String metricsContext, String metricsJmxContext, String haGroupName) {
    super(metricsName, metricsDescription, metricsContext,
      metricsJmxContext + ",haGroup=" + ObjectName.quote(haGroupName));
    getMetricsRegistry().tag(Interns.info(HA_GROUP_TAG_NAME, HA_GROUP_TAG_DESC), haGroupName);

    localCacheHealthStatus = getMetricsRegistry().newGauge(LOCAL_CACHE_HEALTH_STATUS,
      LOCAL_CACHE_HEALTH_STATUS_DESC, 1L);
    peerVisibilityStatus = getMetricsRegistry().newGauge(PEER_VISIBILITY_STATUS,
      PEER_VISIBILITY_STATUS_DESC, 1L);
    degradedStandbyActive =
      getMetricsRegistry().newGauge(DEGRADED_STANDBY_ACTIVE, DEGRADED_STANDBY_ACTIVE_DESC, 0L);
    currentLocalState = getMetricsRegistry().newGauge(CURRENT_LOCAL_STATE, CURRENT_LOCAL_STATE_DESC,
      (long) HAGroupState.UNKNOWN.getMetricCode());
    currentPeerState = getMetricsRegistry().newGauge(CURRENT_PEER_STATE, CURRENT_PEER_STATE_DESC,
      (long) HAGroupState.UNKNOWN.getMetricCode());

    localZkConnectionLostCount = getMetricsRegistry().newCounter(LOCAL_ZK_CONNECTION_LOST_COUNT,
      LOCAL_ZK_CONNECTION_LOST_COUNT_DESC, 0L);
    peerBlindCount = getMetricsRegistry().newCounter(PEER_BLIND_COUNT, PEER_BLIND_COUNT_DESC, 0L);
    degradedStandbyPresentedCount = getMetricsRegistry()
      .newCounter(DEGRADED_STANDBY_PRESENTED_COUNT, DEGRADED_STANDBY_PRESENTED_COUNT_DESC, 0L);
    invalidTransitionRejectedCount = getMetricsRegistry()
      .newCounter(INVALID_TRANSITION_REJECTED_COUNT, INVALID_TRANSITION_REJECTED_COUNT_DESC, 0L);
    systemTableSyncFailedCount = getMetricsRegistry().newCounter(SYSTEM_TABLE_SYNC_FAILED_COUNT,
      SYSTEM_TABLE_SYNC_FAILED_COUNT_DESC, 0L);
    notificationListenerErrorCount = getMetricsRegistry()
      .newCounter(NOTIFICATION_LISTENER_ERROR_COUNT, NOTIFICATION_LISTENER_ERROR_COUNT_DESC, 0L);

    subscriberNotifyTimeMs = getMetricsRegistry().newTimeHistogram(SUBSCRIBER_NOTIFY_TIME_MS,
      SUBSCRIBER_NOTIFY_TIME_MS_DESC);
  }

  @Override
  public void setLocalCacheHealthy(boolean value) {
    localCacheHealthStatus.set(value ? 0L : 1L);
  }

  @Override
  public void setPeerVisible(boolean value) {
    peerVisibilityStatus.set(value ? 0L : 1L);
  }

  @Override
  public void setDegradedStandbyActive(boolean value) {
    degradedStandbyActive.set(value ? 1L : 0L);
  }

  @Override
  public void setCurrentLocalState(HAGroupState state) {
    HAGroupState currentState = state != null ? state : HAGroupState.UNKNOWN;
    currentLocalState.set(currentState.getMetricCode());
  }

  @Override
  public void setCurrentPeerState(HAGroupState state) {
    HAGroupState currentState = state != null ? state : HAGroupState.UNKNOWN;
    currentPeerState.set(currentState.getMetricCode());
  }

  @Override
  public void incrementLocalZkConnectionLostCount() {
    localZkConnectionLostCount.incr();
  }

  @Override
  public void incrementPeerBlindCount() {
    peerBlindCount.incr();
  }

  @Override
  public void incrementDegradedStandbyPresentedCount() {
    degradedStandbyPresentedCount.incr();
  }

  @Override
  public void incrementInvalidTransitionRejectedCount() {
    invalidTransitionRejectedCount.incr();
  }

  @Override
  public void incrementSystemTableSyncFailedCount() {
    systemTableSyncFailedCount.incr();
  }

  @Override
  public void incrementNotificationListenerErrorCount() {
    notificationListenerErrorCount.incr();
  }

  @Override
  public void updateSubscriberNotifyTime(long elapsedTimeNs) {
    subscriberNotifyTimeMs.add(TimeUnit.NANOSECONDS.toMillis(elapsedTimeNs));
  }

  @Override
  public HAGroupStoreMetricValues getCurrentMetricValues() {
    return HAGroupStoreMetricValues.builder()
      .setLocalCacheHealthStatus(localCacheHealthStatus.value())
      .setPeerVisibilityStatus(peerVisibilityStatus.value())
      .setDegradedStandbyActive(degradedStandbyActive.value())
      .setCurrentLocalState(currentLocalState.value()).setCurrentPeerState(currentPeerState.value())
      .setLocalZkConnectionLostCount(localZkConnectionLostCount.value())
      .setPeerBlindCount(peerBlindCount.value())
      .setDegradedStandbyPresentedCount(degradedStandbyPresentedCount.value())
      .setInvalidTransitionRejectedCount(invalidTransitionRejectedCount.value())
      .setSystemTableSyncFailedCount(systemTableSyncFailedCount.value())
      .setNotificationListenerErrorCount(notificationListenerErrorCount.value())
      .setSubscriberNotifyTimeCount(subscriberNotifyTimeMs.getCount())
      .setSubscriberNotifyTimeMaxMs(subscriberNotifyTimeMs.getMax()).build();
  }

  @VisibleForTesting
  public void unregisterForTesting() {
    DefaultMetricsSystem.instance().unregisterSource(metricsJmxContext);
  }
}
