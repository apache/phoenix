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
package org.apache.phoenix.monitoring;

import static org.apache.phoenix.metrics.MetricConstants.HA_GROUP_TAG_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.phoenix.query.QueryServicesOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for {@link HAGroupMetricsManager}: the per-HA-group source registry. Covers per-group
 * isolation (two groups never cross-count), that each group registers as its own
 * {@code ha_group}-tagged metrics2 source, and that {@link HAGroupMetricsManager#remove(String)}
 * detaches the source so the same group can be re-created fresh.
 * <p>
 * These tests mutate process-global state ({@link DefaultMetricsSystem} and the manager's static
 * map), so each group name is unique per test and {@link #tearDown()} removes every group this
 * class created to avoid leaking sources into sibling tests.
 */
public class HAGroupMetricsManagerTest {

  private final List<String> createdGroups = new ArrayList<>();

  @BeforeClass
  public static void assumeGlobalMetricsEnabled() {
    // The manager creates sources only when global metrics are enabled (the default). If a
    // surrounding config disabled them, getOrCreate would return null and these assertions would
    // not apply.
    assertTrue("these tests assume the default global-metrics-enabled=true",
      QueryServicesOptions.withDefaults().isGlobalMetricsEnabled());
  }

  @Before
  public void setUp() {
    createdGroups.clear();
  }

  @After
  public void tearDown() {
    for (String group : createdGroups) {
      HAGroupMetricsManager.remove(group);
    }
  }

  private String track(String group) {
    createdGroups.add(group);
    return group;
  }

  @Test
  public void testGetOrCreateIsIdempotentAndRegistersSource() {
    String group = track("mgrCreate");
    HAGroupClientMetricsSource first = HAGroupMetricsManager.getOrCreate(group);
    HAGroupClientMetricsSource second = HAGroupMetricsManager.getOrCreate(group);
    assertNotNull(first);
    assertSame("getOrCreate must be idempotent for a group name", first, second);
    assertNotNull("the group's source must be registered with DefaultMetricsSystem",
      DefaultMetricsSystem.instance().getSource(first.getMetricsJmxContext()));
    assertEquals("the source must carry the ha_group tag", group,
      first.getMetricsRegistry().getTag(HA_GROUP_TAG_NAME).value());
  }

  @Test
  public void testNullAndEmptyGroupNameIsNoOp() {
    assertNull(HAGroupMetricsManager.getOrCreate(null));
    assertNull(HAGroupMetricsManager.getOrCreate(""));
    // remove must tolerate null/empty without throwing.
    HAGroupMetricsManager.remove(null);
    HAGroupMetricsManager.remove("");
  }

  @Test
  public void testTwoGroupsDoNotCrossCount() {
    String groupA = track("mgrIsoA");
    String groupB = track("mgrIsoB");
    MetricType type = MetricType.HA_FAILOVER_CONNECTION_FAILED_COUNTER;

    HAGroupMetricsManager.increment(groupA, type);
    HAGroupMetricsManager.increment(groupA, type);
    HAGroupMetricsManager.increment(groupB, type);

    assertEquals(2L, HAGroupMetricsManager.getIfPresent(groupA).getCounterValue(type));
    assertEquals("each HA group must maintain independent counters", 1L,
      HAGroupMetricsManager.getIfPresent(groupB).getCounterValue(type));
  }

  @Test
  public void testEachGroupGetsADistinctTaggedSource() {
    String groupA = track("mgrDistinctA");
    String groupB = track("mgrDistinctB");
    HAGroupClientMetricsSource regA = HAGroupMetricsManager.getOrCreate(groupA);
    HAGroupClientMetricsSource regB = HAGroupMetricsManager.getOrCreate(groupB);

    assertNotEquals("distinct groups must map to distinct JMX contexts",
      regA.getMetricsJmxContext(), regB.getMetricsJmxContext());
    assertEquals(groupA, regA.getMetricsRegistry().getTag(HA_GROUP_TAG_NAME).value());
    assertEquals(groupB, regB.getMetricsRegistry().getTag(HA_GROUP_TAG_NAME).value());
  }

  @Test
  public void testUpdateAccumulatesPerGroupDuration() {
    String group = track("mgrDuration");
    MetricType type = MetricType.CRR_TRANSITION_DURATION_MS;
    HAGroupMetricsManager.update(group, type, 40L);
    HAGroupMetricsManager.update(group, type, 60L);
    assertEquals(100L, HAGroupMetricsManager.getIfPresent(group).getCounterValue(type));
  }

  @Test
  public void testRemoveDetachesSource() {
    String group = "mgrRemove"; // not tracked: this test removes it itself
    HAGroupClientMetricsSource source = HAGroupMetricsManager.getOrCreate(group);
    String jmxContext = source.getMetricsJmxContext();
    assertNotNull(HAGroupMetricsManager.getIfPresent(group));
    assertNotNull(DefaultMetricsSystem.instance().getSource(jmxContext));

    HAGroupMetricsManager.remove(group);

    assertNull("holder must be gone after remove", HAGroupMetricsManager.getIfPresent(group));
    assertNull("source must be detached from DefaultMetricsSystem after remove",
      DefaultMetricsSystem.instance().getSource(jmxContext));
  }

  @Test
  public void testGetOrCreateAfterRemoveRebuildsFresh() {
    String group = track("mgrRebuild");
    MetricType type = MetricType.HA_STALE_CRR_DETECTED_COUNT;
    HAGroupMetricsManager.increment(group, type);
    assertEquals(1L, HAGroupMetricsManager.getIfPresent(group).getCounterValue(type));

    HAGroupMetricsManager.remove(group);
    // A re-create after remove must yield a fresh source with zeroed counters.
    HAGroupClientMetricsSource rebuilt = HAGroupMetricsManager.getOrCreate(group);
    assertNotNull(rebuilt);
    assertEquals("re-created group must start from zero", 0L, rebuilt.getCounterValue(type));
  }
}
