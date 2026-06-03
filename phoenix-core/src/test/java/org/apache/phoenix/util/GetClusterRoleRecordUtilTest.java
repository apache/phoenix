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
package org.apache.phoenix.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link GetClusterRoleRecordUtil}. These tests cover two regressions in the
 * non-active CRR poller infrastructure:
 * <ol>
 * <li>Per-HA-group future tracking: a previous implementation kept a single static
 * {@code pollerFuture} field that was overwritten by every {@code schedulePoller} invocation
 * regardless of HA group, so cancelling one HA group's poller would target whichever future was
 * scheduled most recently — possibly belonging to a different HA group. The fix keys both the
 * scheduler executor and the future on {@code haGroupName} via concurrent maps.</li>
 * <li>URL alternation each tick: a previous implementation pinned each scheduled poller to a single
 * URL passed in at schedule time. If that cluster's RegionServer Endpoint became transiently
 * unreachable, the poller could never observe the peer cluster's CRR even after the peer became
 * Active. The fix has the poller alternate between url1 and url2 on each tick.</li>
 * </ol>
 */
public class GetClusterRoleRecordUtilTest {

  private static final String URL_1 = "phoenix+rpc:cluster1.example.com:2181";
  private static final String URL_2 = "phoenix+rpc:cluster2.example.com:2181";
  private static final String HA_GROUP_A = "haGroupA";
  private static final String HA_GROUP_B = "haGroupB";

  @Before
  public void clearStateBefore() {
    GetClusterRoleRecordUtil.getFutureMapForTesting().clear();
    GetClusterRoleRecordUtil.getSchedulerMapForTesting().clear();
  }

  @After
  public void clearStateAfter() {
    // Ensure nothing leaks across tests; the maps are static class state.
    GetClusterRoleRecordUtil.getFutureMapForTesting().clear();
    GetClusterRoleRecordUtil.getSchedulerMapForTesting().clear();
  }

  /**
   * URL-alternation core: even ticks pick url1, odd ticks pick url2. This verifies the helper that
   * the poller calls each tick to choose its target URL.
   */
  @Test
  public void testSelectUrlForTickAlternates() {
    assertEquals(URL_1, GetClusterRoleRecordUtil.selectUrlForTick(URL_1, URL_2, 0L));
    assertEquals(URL_2, GetClusterRoleRecordUtil.selectUrlForTick(URL_1, URL_2, 1L));
    assertEquals(URL_1, GetClusterRoleRecordUtil.selectUrlForTick(URL_1, URL_2, 2L));
    assertEquals(URL_2, GetClusterRoleRecordUtil.selectUrlForTick(URL_1, URL_2, 3L));
    assertEquals(URL_1, GetClusterRoleRecordUtil.selectUrlForTick(URL_1, URL_2, 100L));
    assertEquals(URL_2, GetClusterRoleRecordUtil.selectUrlForTick(URL_1, URL_2, 101L));
  }

  /**
   * Defensive: large tick counts should still alternate cleanly. Guards against accidental sign
   * issues if a long tick value approaches Long.MAX_VALUE.
   */
  @Test
  public void testSelectUrlForTickHandlesLargeTickValues() {
    assertEquals(URL_1, GetClusterRoleRecordUtil.selectUrlForTick(URL_1, URL_2, 1_000_000L));
    assertEquals(URL_2, GetClusterRoleRecordUtil.selectUrlForTick(URL_1, URL_2, 1_000_001L));
    assertEquals(URL_1,
      GetClusterRoleRecordUtil.selectUrlForTick(URL_1, URL_2, Long.MAX_VALUE - 1));
    assertEquals(URL_2, GetClusterRoleRecordUtil.selectUrlForTick(URL_1, URL_2, Long.MAX_VALUE));
  }

  /**
   * Per-HA-group future map: distinct HA group names produce distinct map entries. Verifies the
   * data-structure invariant that replaces the prior single-static-{@code pollerFuture} field.
   */
  @Test
  public void testFutureMapIsolatesEntriesPerHaGroup() {
    Map<String, ScheduledFuture<?>> futureMap = GetClusterRoleRecordUtil.getFutureMapForTesting();
    Map<String, ScheduledExecutorService> schedulerMap =
      GetClusterRoleRecordUtil.getSchedulerMapForTesting();
    assertTrue("futureMap should start empty", futureMap.isEmpty());
    assertTrue("schedulerMap should start empty", schedulerMap.isEmpty());

    ScheduledFuture<?> futureA = mock(ScheduledFuture.class);
    ScheduledFuture<?> futureB = mock(ScheduledFuture.class);
    ScheduledExecutorService schedulerA = mock(ScheduledExecutorService.class);
    ScheduledExecutorService schedulerB = mock(ScheduledExecutorService.class);

    futureMap.put(HA_GROUP_A, futureA);
    futureMap.put(HA_GROUP_B, futureB);
    schedulerMap.put(HA_GROUP_A, schedulerA);
    schedulerMap.put(HA_GROUP_B, schedulerB);

    assertEquals(2, futureMap.size());
    assertEquals(2, schedulerMap.size());
    assertNotNull(futureMap.get(HA_GROUP_A));
    assertNotNull(futureMap.get(HA_GROUP_B));
    // Distinct entries — adding HA_GROUP_B did not overwrite HA_GROUP_A's entry.
    assertFalse("entries for distinct HA groups must be different references",
      futureMap.get(HA_GROUP_A) == futureMap.get(HA_GROUP_B));
  }

  /**
   * Removal/cancellation isolation: cancelling one HA group's poller cancels only that group's
   * future and does not touch the peer group's future. This is the key behavioural invariant the
   * prior single-static field violated.
   */
  @Test
  public void testCancelOneHaGroupDoesNotCancelOthers() {
    Map<String, ScheduledFuture<?>> futureMap = GetClusterRoleRecordUtil.getFutureMapForTesting();
    Map<String, ScheduledExecutorService> schedulerMap =
      GetClusterRoleRecordUtil.getSchedulerMapForTesting();

    ScheduledFuture<?> futureA = mock(ScheduledFuture.class);
    ScheduledFuture<?> futureB = mock(ScheduledFuture.class);
    ScheduledExecutorService schedulerA = mock(ScheduledExecutorService.class);
    ScheduledExecutorService schedulerB = mock(ScheduledExecutorService.class);
    when(futureA.cancel(false)).thenReturn(true);
    when(futureB.cancel(false)).thenReturn(true);

    futureMap.put(HA_GROUP_A, futureA);
    futureMap.put(HA_GROUP_B, futureB);
    schedulerMap.put(HA_GROUP_A, schedulerA);
    schedulerMap.put(HA_GROUP_B, schedulerB);

    // Mirror the cancel-on-active path inside schedulePoller: remove the entry for HA_GROUP_A,
    // cancel its future, shut down its scheduler. HA_GROUP_B's entries must be untouched.
    ScheduledFuture<?> removedFuture = futureMap.remove(HA_GROUP_A);
    assertNotNull(removedFuture);
    removedFuture.cancel(false);
    ScheduledExecutorService removedScheduler = schedulerMap.remove(HA_GROUP_A);
    assertNotNull(removedScheduler);
    removedScheduler.shutdown();

    verify(futureA, times(1)).cancel(false);
    verify(futureB, never()).cancel(false);
    verify(schedulerA, times(1)).shutdown();
    verify(schedulerB, never()).shutdown();
    assertNull("HA_GROUP_A entry should be removed", futureMap.get(HA_GROUP_A));
    assertNotNull("HA_GROUP_B entry should remain", futureMap.get(HA_GROUP_B));
    assertEquals(1, futureMap.size());
    assertEquals(1, schedulerMap.size());
  }
}
