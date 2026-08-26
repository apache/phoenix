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

import static org.apache.phoenix.jdbc.metrics.HAGroupStoreMetricsSource.CURRENT_LOCAL_STATE;
import static org.apache.phoenix.jdbc.metrics.HAGroupStoreMetricsSource.CURRENT_PEER_STATE;
import static org.apache.phoenix.jdbc.metrics.HAGroupStoreMetricsSource.DEGRADED_STANDBY_ACTIVE;
import static org.apache.phoenix.jdbc.metrics.HAGroupStoreMetricsSource.DEGRADED_STANDBY_PRESENTED_COUNT;
import static org.apache.phoenix.jdbc.metrics.HAGroupStoreMetricsSource.INVALID_TRANSITION_REJECTED_COUNT;
import static org.apache.phoenix.jdbc.metrics.HAGroupStoreMetricsSource.LOCAL_CACHE_HEALTH_STATUS;
import static org.apache.phoenix.jdbc.metrics.HAGroupStoreMetricsSource.LOCAL_ZK_CONNECTION_LOST_COUNT;
import static org.apache.phoenix.jdbc.metrics.HAGroupStoreMetricsSource.NOTIFICATION_LISTENER_ERROR_COUNT;
import static org.apache.phoenix.jdbc.metrics.HAGroupStoreMetricsSource.PEER_BLIND_COUNT;
import static org.apache.phoenix.jdbc.metrics.HAGroupStoreMetricsSource.PEER_VISIBILITY_STATUS;
import static org.apache.phoenix.jdbc.metrics.HAGroupStoreMetricsSource.SUBSCRIBER_NOTIFY_TIME_MS;
import static org.apache.phoenix.jdbc.metrics.HAGroupStoreMetricsSource.SUBSCRIBER_NOTIFY_TIME_MS_EXPORTED_BASE;
import static org.apache.phoenix.jdbc.metrics.HAGroupStoreMetricsSource.SYSTEM_TABLE_SYNC_FAILED_COUNT;
import static org.apache.phoenix.metrics.MetricConstants.HA_GROUP_TAG_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.management.ObjectName;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.impl.MetricsRecordImpl;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableMetric;
import org.apache.hadoop.metrics2.lib.MutableTimeHistogram;
import org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HAGroupStoreMetricsSourceImplTest {

  private HAGroupStoreMetricsSourceImpl source;

  @Before
  public void setUp() {
    source = new HAGroupStoreMetricsSourceImpl("metrics-test-" + System.nanoTime());
  }

  @After
  public void tearDown() {
    source.unregisterForTesting();
  }

  @Test
  public void testMetricNamesAreUniqueAndPrefixed() {
    Set<String> metricNames = new HashSet<>(Arrays.asList(LOCAL_CACHE_HEALTH_STATUS,
      PEER_VISIBILITY_STATUS, DEGRADED_STANDBY_ACTIVE, CURRENT_LOCAL_STATE, CURRENT_PEER_STATE,
      LOCAL_ZK_CONNECTION_LOST_COUNT, PEER_BLIND_COUNT, DEGRADED_STANDBY_PRESENTED_COUNT,
      INVALID_TRANSITION_REJECTED_COUNT, SYSTEM_TABLE_SYNC_FAILED_COUNT,
      NOTIFICATION_LISTENER_ERROR_COUNT, SUBSCRIBER_NOTIFY_TIME_MS));

    assertEquals(12, metricNames.size());
    for (String metricName : metricNames) {
      assertTrue(metricName, metricName.startsWith("haGroupStore"));
    }
  }

  @Test
  public void testMetricRegistryContract() {
    Map<String, MutableMetric> metrics = source.getMetricsRegistry().getMetricsMap();
    assertEquals(12, metrics.size());

    for (String gauge : Arrays.asList(LOCAL_CACHE_HEALTH_STATUS, PEER_VISIBILITY_STATUS,
      DEGRADED_STANDBY_ACTIVE, CURRENT_LOCAL_STATE, CURRENT_PEER_STATE)) {
      assertTrue(gauge, metrics.get(gauge) instanceof MutableGaugeLong);
    }
    for (String counter : Arrays.asList(LOCAL_ZK_CONNECTION_LOST_COUNT, PEER_BLIND_COUNT,
      DEGRADED_STANDBY_PRESENTED_COUNT, INVALID_TRANSITION_REJECTED_COUNT,
      SYSTEM_TABLE_SYNC_FAILED_COUNT, NOTIFICATION_LISTENER_ERROR_COUNT)) {
      assertTrue(counter, metrics.get(counter) instanceof MutableFastCounter);
    }
    assertTrue(metrics.get(SUBSCRIBER_NOTIFY_TIME_MS) instanceof MutableTimeHistogram);
  }

  @Test
  public void testMetricsSnapshot() {
    source.setLocalCacheHealthy(true);
    source.setPeerVisible(true);
    source.setDegradedStandbyActive(true);
    source.setCurrentLocalState(HAGroupState.STANDBY_TO_ACTIVE);
    source.setCurrentPeerState(HAGroupState.ACTIVE_IN_SYNC);
    source.incrementLocalZkConnectionLostCount();
    source.incrementPeerBlindCount();
    source.incrementDegradedStandbyPresentedCount();
    source.incrementInvalidTransitionRejectedCount();
    source.incrementSystemTableSyncFailedCount();
    source.incrementNotificationListenerErrorCount();
    source.updateSubscriberNotifyTime(5_000_000L);

    HAGroupStoreMetricValues values = source.getCurrentMetricValues();
    assertEquals(0L, values.getLocalCacheHealthStatus());
    assertEquals(0L, values.getPeerVisibilityStatus());
    assertEquals(1L, values.getDegradedStandbyActive());
    assertEquals(HAGroupState.STANDBY_TO_ACTIVE.getMetricCode(), values.getCurrentLocalState());
    assertEquals(HAGroupState.ACTIVE_IN_SYNC.getMetricCode(), values.getCurrentPeerState());
    assertEquals(1L, values.getLocalZkConnectionLostCount());
    assertEquals(1L, values.getPeerBlindCount());
    assertEquals(1L, values.getDegradedStandbyPresentedCount());
    assertEquals(1L, values.getInvalidTransitionRejectedCount());
    assertEquals(1L, values.getSystemTableSyncFailedCount());
    assertEquals(1L, values.getNotificationListenerErrorCount());
    assertEquals(1L, values.getSubscriberNotifyTimeCount());
    assertEquals(5L, values.getSubscriberNotifyTimeMaxMs());
  }

  @Test
  public void testSubMillisecondNotifyTimeRoundsDownToZero() {
    source.updateSubscriberNotifyTime(999_999L);

    HAGroupStoreMetricValues values = source.getCurrentMetricValues();
    assertEquals(1L, values.getSubscriberNotifyTimeCount());
    assertEquals(0L, values.getSubscriberNotifyTimeMaxMs());
  }

  @Test
  public void testExportedHistogramNames() {
    source.updateSubscriberNotifyTime(5_000_000L);
    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    source.getMetrics(collector, true);

    Set<String> exportedNames = new HashSet<>();
    for (MetricsRecordImpl record : collector.getRecords()) {
      for (AbstractMetric metric : record.metrics()) {
        exportedNames.add(metric.name());
      }
    }

    assertTrue(exportedNames.contains(SUBSCRIBER_NOTIFY_TIME_MS_EXPORTED_BASE + "_num_ops"));
    assertTrue(exportedNames.contains(SUBSCRIBER_NOTIFY_TIME_MS_EXPORTED_BASE + "_max"));
    assertTrue(
      exportedNames.contains(SUBSCRIBER_NOTIFY_TIME_MS_EXPORTED_BASE + "_99th_percentile"));
    assertFalse(exportedNames.contains(SUBSCRIBER_NOTIFY_TIME_MS + "_num_ops"));
  }

  @Test
  public void testFactoryCachesSourceByGroup() {
    String group = "factory-test-" + System.nanoTime();
    assertSame(HAGroupStoreMetricsSourceFactory.getInstanceForHAGroup(group),
      HAGroupStoreMetricsSourceFactory.getInstanceForHAGroup(group));
  }

  @Test
  public void testFactoryIsolatesGroups() {
    String prefix = "factory-isolation-" + System.nanoTime();
    HAGroupStoreMetricsSource first =
      HAGroupStoreMetricsSourceFactory.getInstanceForHAGroup(prefix + "-1");
    HAGroupStoreMetricsSource second =
      HAGroupStoreMetricsSourceFactory.getInstanceForHAGroup(prefix + "-2");

    assertNotSame(first, second);
    assertNotEquals(first.getMetricsJmxContext(), second.getMetricsJmxContext());
    first.incrementInvalidTransitionRejectedCount();
    assertEquals(1L, first.getCurrentMetricValues().getInvalidTransitionRejectedCount());
    assertEquals(0L, second.getCurrentMetricValues().getInvalidTransitionRejectedCount());
  }

  @Test
  public void testGroupIdentityIsTaggedAndQuoted() {
    String group = "group,one=" + System.nanoTime();
    HAGroupStoreMetricsSourceImpl taggedSource = new HAGroupStoreMetricsSourceImpl(group);
    try {
      assertEquals(group, taggedSource.getMetricsRegistry().getTag(HA_GROUP_TAG_NAME).value());
      assertTrue(
        taggedSource.getMetricsJmxContext().endsWith(",haGroup=" + ObjectName.quote(group)));
    } finally {
      taggedSource.unregisterForTesting();
    }
  }
}
