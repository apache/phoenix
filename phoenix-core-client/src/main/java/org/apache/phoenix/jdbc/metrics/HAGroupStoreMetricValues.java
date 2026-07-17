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

/** Immutable snapshot of HAGroupStore metrics used by tests. */
public final class HAGroupStoreMetricValues {

  private final long localCacheHealthStatus;
  private final long peerVisibilityStatus;
  private final long degradedStandbyActive;
  private final long currentLocalState;
  private final long currentPeerState;
  private final long localZkConnectionLostCount;
  private final long peerBlindCount;
  private final long degradedStandbyPresentedCount;
  private final long invalidTransitionRejectedCount;
  private final long systemTableSyncFailedCount;
  private final long notificationListenerErrorCount;
  private final long subscriberNotifyTimeCount;
  private final long subscriberNotifyTimeMaxMs;

  private HAGroupStoreMetricValues(Builder builder) {
    localCacheHealthStatus = builder.localCacheHealthStatus;
    peerVisibilityStatus = builder.peerVisibilityStatus;
    degradedStandbyActive = builder.degradedStandbyActive;
    currentLocalState = builder.currentLocalState;
    currentPeerState = builder.currentPeerState;
    localZkConnectionLostCount = builder.localZkConnectionLostCount;
    peerBlindCount = builder.peerBlindCount;
    degradedStandbyPresentedCount = builder.degradedStandbyPresentedCount;
    invalidTransitionRejectedCount = builder.invalidTransitionRejectedCount;
    systemTableSyncFailedCount = builder.systemTableSyncFailedCount;
    notificationListenerErrorCount = builder.notificationListenerErrorCount;
    subscriberNotifyTimeCount = builder.subscriberNotifyTimeCount;
    subscriberNotifyTimeMaxMs = builder.subscriberNotifyTimeMaxMs;
  }

  public static Builder builder() {
    return new Builder();
  }

  public long getLocalCacheHealthStatus() {
    return localCacheHealthStatus;
  }

  public long getPeerVisibilityStatus() {
    return peerVisibilityStatus;
  }

  public long getDegradedStandbyActive() {
    return degradedStandbyActive;
  }

  public long getCurrentLocalState() {
    return currentLocalState;
  }

  public long getCurrentPeerState() {
    return currentPeerState;
  }

  public long getLocalZkConnectionLostCount() {
    return localZkConnectionLostCount;
  }

  public long getPeerBlindCount() {
    return peerBlindCount;
  }

  public long getDegradedStandbyPresentedCount() {
    return degradedStandbyPresentedCount;
  }

  public long getInvalidTransitionRejectedCount() {
    return invalidTransitionRejectedCount;
  }

  public long getSystemTableSyncFailedCount() {
    return systemTableSyncFailedCount;
  }

  public long getNotificationListenerErrorCount() {
    return notificationListenerErrorCount;
  }

  public long getSubscriberNotifyTimeCount() {
    return subscriberNotifyTimeCount;
  }

  public long getSubscriberNotifyTimeMaxMs() {
    return subscriberNotifyTimeMaxMs;
  }

  public static final class Builder {
    private long localCacheHealthStatus;
    private long peerVisibilityStatus;
    private long degradedStandbyActive;
    private long currentLocalState;
    private long currentPeerState;
    private long localZkConnectionLostCount;
    private long peerBlindCount;
    private long degradedStandbyPresentedCount;
    private long invalidTransitionRejectedCount;
    private long systemTableSyncFailedCount;
    private long notificationListenerErrorCount;
    private long subscriberNotifyTimeCount;
    private long subscriberNotifyTimeMaxMs;

    public Builder setLocalCacheHealthStatus(long value) {
      localCacheHealthStatus = value;
      return this;
    }

    public Builder setPeerVisibilityStatus(long value) {
      peerVisibilityStatus = value;
      return this;
    }

    public Builder setDegradedStandbyActive(long value) {
      degradedStandbyActive = value;
      return this;
    }

    public Builder setCurrentLocalState(long value) {
      currentLocalState = value;
      return this;
    }

    public Builder setCurrentPeerState(long value) {
      currentPeerState = value;
      return this;
    }

    public Builder setLocalZkConnectionLostCount(long value) {
      localZkConnectionLostCount = value;
      return this;
    }

    public Builder setPeerBlindCount(long value) {
      peerBlindCount = value;
      return this;
    }

    public Builder setDegradedStandbyPresentedCount(long value) {
      degradedStandbyPresentedCount = value;
      return this;
    }

    public Builder setInvalidTransitionRejectedCount(long value) {
      invalidTransitionRejectedCount = value;
      return this;
    }

    public Builder setSystemTableSyncFailedCount(long value) {
      systemTableSyncFailedCount = value;
      return this;
    }

    public Builder setNotificationListenerErrorCount(long value) {
      notificationListenerErrorCount = value;
      return this;
    }

    public Builder setSubscriberNotifyTimeCount(long value) {
      subscriberNotifyTimeCount = value;
      return this;
    }

    public Builder setSubscriberNotifyTimeMaxMs(long value) {
      subscriberNotifyTimeMaxMs = value;
      return this;
    }

    public HAGroupStoreMetricValues build() {
      return new HAGroupStoreMetricValues(this);
    }
  }
}
