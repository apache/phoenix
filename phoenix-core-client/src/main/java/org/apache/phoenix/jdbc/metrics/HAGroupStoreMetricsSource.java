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

import org.apache.hadoop.hbase.metrics.BaseSource;
import org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Metrics for one HAGroupStore group on a RegionServer.
 * <p>
 * Lifecycle gauges are best-effort and re-baselined when a client is constructed. Counters and the
 * histogram operation count are cumulative for the JVM lifetime; histogram distributions cover the
 * latest Metrics2 collection interval. Metrics are not transactionally coordinated across client
 * replacement or RegionServers and never feed HA decisions.
 * <p>
 * {@code ha_group} is the only source-specific tag. Monitoring systems must supply cluster,
 * environment, host, and RegionServer identity as external scrape-target labels; role is mutable
 * and is represented by the state gauges rather than a tag.
 * <p>
 * State gauge code mapping:
 * <ul>
 * <li>0 = UNKNOWN</li>
 * <li>1 = ABORT_TO_ACTIVE_IN_SYNC</li>
 * <li>2 = ABORT_TO_ACTIVE_NOT_IN_SYNC</li>
 * <li>3 = ABORT_TO_STANDBY</li>
 * <li>4 = ACTIVE_IN_SYNC</li>
 * <li>5 = ACTIVE_NOT_IN_SYNC</li>
 * <li>6 = ACTIVE_NOT_IN_SYNC_TO_STANDBY</li>
 * <li>7 = ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER</li>
 * <li>8 = ACTIVE_IN_SYNC_TO_STANDBY</li>
 * <li>9 = ACTIVE_WITH_OFFLINE_PEER</li>
 * <li>10 = DEGRADED_STANDBY</li>
 * <li>11 = OFFLINE</li>
 * <li>12 = STANDBY</li>
 * <li>13 = STANDBY_TO_ACTIVE</li>
 * </ul>
 */
public interface HAGroupStoreMetricsSource extends BaseSource {

  String METRICS_NAME = "HAGroupStore";
  String METRICS_CONTEXT = "phoenix";
  String METRICS_DESCRIPTION = "Metrics for HAGroupStore operations";
  // Server-oriented JMX identity consistent with sibling HBase/Phoenix sources. Short-lived admin
  // CLI invocations can create an incidental bean under the same context.
  String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

  String LOCAL_CACHE_HEALTH_STATUS = "haGroupStoreLocalCacheHealthStatus";
  String LOCAL_CACHE_HEALTH_STATUS_DESC =
    "Local HAGroupStore cache health status: 0 healthy, non-zero unhealthy";
  String PEER_VISIBILITY_STATUS = "haGroupStorePeerVisibilityStatus";
  String PEER_VISIBILITY_STATUS_DESC =
    "Configured peer visibility status: 0 visible or unconfigured, non-zero blind";
  String DEGRADED_STANDBY_ACTIVE = "haGroupStoreDegradedStandbyActive";
  String DEGRADED_STANDBY_ACTIVE_DESC =
    "Local in-memory peer-blind fail-closed overlay: 0 inactive, 1 active on raw STANDBY; "
      + "never persisted to ZooKeeper and distinct from persisted DEGRADED_STANDBY";
  String CURRENT_LOCAL_STATE = "haGroupStoreCurrentLocalState";
  String CURRENT_LOCAL_STATE_DESC = "Current raw local HA group state code";
  String CURRENT_PEER_STATE = "haGroupStoreCurrentPeerState";
  String CURRENT_PEER_STATE_DESC = "Current peer HA group state code";

  String LOCAL_ZK_CONNECTION_LOST_COUNT = "haGroupStoreLocalZkConnectionLostCount";
  String LOCAL_ZK_CONNECTION_LOST_COUNT_DESC = "Number of local ZooKeeper connection losses";
  String PEER_BLIND_COUNT = "haGroupStorePeerBlindCount";
  String PEER_BLIND_COUNT_DESC = "Number of transitions to peer-blind";
  String DEGRADED_STANDBY_PRESENTED_COUNT = "haGroupStoreDegradedStandbyPresentedCount";
  String DEGRADED_STANDBY_PRESENTED_COUNT_DESC = "Number of degraded standby overlay presentations";
  String INVALID_TRANSITION_REJECTED_COUNT = "haGroupStoreInvalidTransitionRejectedCount";
  String INVALID_TRANSITION_REJECTED_COUNT_DESC =
    "Number of invalid HA group state transitions rejected";
  String SYSTEM_TABLE_SYNC_FAILED_COUNT = "haGroupStoreSystemTableSyncFailedCount";
  String SYSTEM_TABLE_SYNC_FAILED_COUNT_DESC =
    "Number of failed SYSTEM.HA_GROUP synchronization attempts";
  String NOTIFICATION_LISTENER_ERROR_COUNT = "haGroupStoreNotificationListenerErrorCount";
  String NOTIFICATION_LISTENER_ERROR_COUNT_DESC = "Number of HA group notification listener errors";

  // MutableTimeHistogram capitalizes its exported basename. The logical registry key remains
  // lowercase, while JMX/Metrics2 consumers use SUBSCRIBER_NOTIFY_TIME_MS_EXPORTED_BASE plus the
  // standard histogram suffixes.
  String SUBSCRIBER_NOTIFY_TIME_MS = "haGroupStoreSubscriberNotifyTimeMs";
  String SUBSCRIBER_NOTIFY_TIME_MS_EXPORTED_BASE = "HaGroupStoreSubscriberNotifyTimeMs";
  String SUBSCRIBER_NOTIFY_TIME_MS_DESC =
    "Time spent synchronously notifying HA group subscribers in whole milliseconds; "
      + "durations below one millisecond are recorded as zero-valued samples and still increment "
      + "the operation count";

  void setLocalCacheHealthy(boolean healthy);

  void setPeerVisible(boolean visible);

  void setDegradedStandbyActive(boolean active);

  void setCurrentLocalState(HAGroupState state);

  void setCurrentPeerState(HAGroupState state);

  void incrementLocalZkConnectionLostCount();

  void incrementPeerBlindCount();

  void incrementDegradedStandbyPresentedCount();

  void incrementInvalidTransitionRejectedCount();

  void incrementSystemTableSyncFailedCount();

  void incrementNotificationListenerErrorCount();

  void updateSubscriberNotifyTime(long elapsedTimeNs);

  @VisibleForTesting
  HAGroupStoreMetricValues getCurrentMetricValues();
}
