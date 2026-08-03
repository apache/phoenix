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
package org.apache.phoenix.coprocessor.tasks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Timestamp;
import org.apache.phoenix.schema.transform.SystemTransformRecord;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.junit.Test;

/**
 * Unit coverage for the partial-pass wait arithmetic in {@link TransformMonitorTask}. The wait is
 * added to the current time to form a persisted deadline, so the arithmetic must never overflow
 * into a negative (past) deadline. In particular a table configured to never refresh its cache
 * reports an update-cache-frequency of {@link Long#MAX_VALUE}; scaling that unbounded would
 * saturate and, once added to the current time, wrap negative and defeat the wait entirely. These
 * assertions pin the clamp-before-scale behavior deterministically, without standing up a cluster.
 */
public class TransformMonitorTaskWaitTest {

  private static final long MIN_WAIT_MS = 30L * 60L * 1000L;
  private static final long MAX_WAIT_MS = 24L * 60L * 60L * 1000L;

  @Test
  public void testNeverCachedFrequencyClampsToCeilingNotOverflow() {
    // A never-refreshed table resolves update-cache-frequency to Long.MAX_VALUE.
    long wait = TransformMonitorTask.boundedPartialPassWaitMs(Long.MAX_VALUE);
    assertEquals("A never-refreshed table must clamp to the 24h ceiling, not overflow", MAX_WAIT_MS,
      wait);
  }

  @Test
  public void testZeroAndSmallFrequencyFloorToMinimum() {
    assertEquals("Zero frequency floors to the minimum wait", MIN_WAIT_MS,
      TransformMonitorTask.boundedPartialPassWaitMs(0));
    assertEquals("A frequency below the floor (after scaling) floors to the minimum wait",
      MIN_WAIT_MS, TransformMonitorTask.boundedPartialPassWaitMs(1000));
  }

  @Test
  public void testNegativeFrequencyFloorsToMinimum() {
    // Defensive: a negative frequency should never yield a negative or past deadline.
    assertEquals("A negative frequency floors to the minimum wait", MIN_WAIT_MS,
      TransformMonitorTask.boundedPartialPassWaitMs(-1L));
    assertEquals("Long.MIN_VALUE floors to the minimum wait", MIN_WAIT_MS,
      TransformMonitorTask.boundedPartialPassWaitMs(Long.MIN_VALUE));
  }

  @Test
  public void testMidRangeFrequencyScalesWithSafetyMargin() {
    // A one-hour cache frequency, well inside the window, scales by the 1.10 safety margin.
    long oneHour = 60L * 60L * 1000L;
    long wait = TransformMonitorTask.boundedPartialPassWaitMs(oneHour);
    assertEquals("A mid-range frequency scales by the 1.10 safety margin", (long) (oneHour * 1.10),
      wait);
  }

  @Test
  public void testFrequencyAtOrAboveCeilingClampsToCeiling() {
    assertEquals("A frequency exactly at the ceiling clamps to the ceiling", MAX_WAIT_MS,
      TransformMonitorTask.boundedPartialPassWaitMs(MAX_WAIT_MS));
    assertEquals("A frequency above the ceiling clamps to the ceiling", MAX_WAIT_MS,
      TransformMonitorTask.boundedPartialPassWaitMs(MAX_WAIT_MS + 1));
  }

  private static SystemTransformRecord recordWith(Long cutoverTs, Long lastStateTs) {
    SystemTransformRecord.SystemTransformBuilder builder =
      new SystemTransformRecord.SystemTransformBuilder();
    builder.setCutoverTs(cutoverTs);
    builder.setLastStateTs(lastStateTs == null ? null : new Timestamp(lastStateTs));
    return builder.build();
  }

  @Test
  public void testRepairScanFloorUsesCutoverInstantNotPostWaitLastStateTs() {
    // Regression guard for the strand/data-loss bug: lastStateTs is stamped AFTER the post-cutover
    // wait window, so a floor derived from it (5000 - 1) would skip every row written to the old
    // pointer during [cutover, cutover + waitWindow]. The floor must instead track the cutover
    // instant (1000 - 1). This is the exact scenario the fix exists to prevent.
    long cutoverTs = 1000L;
    long postWaitLastStateTs = 5000L;
    long floor = TransformMonitorTask.repairScanFloor(recordWith(cutoverTs, postWaitLastStateTs));
    assertEquals("Repair-scan floor must track the cutover instant, not the post-wait lastStateTs",
      cutoverTs - 1, floor);
  }

  @Test
  public void testRepairScanFloorFallsBackToLastStateTsForLegacyRecords() {
    // Records predating the CUTOVER_TS column carry a null cutoverTs; preserve the prior behavior
    // (floor derived from lastStateTs) rather than rescanning the whole table.
    long lastStateTs = 5000L;
    long floor = TransformMonitorTask.repairScanFloor(recordWith(null, lastStateTs));
    assertEquals("With no cutover instant, the floor falls back to lastStateTs", lastStateTs - 1,
      floor);
  }

  @Test
  public void testRepairScanFloorFullScanWhenNeitherSet() {
    assertEquals("With neither timestamp set, the floor is 0 (full scan)", 0L,
      TransformMonitorTask.repairScanFloor(recordWith(null, null)));
  }

  @Test
  public void testResolveCutoverTsReusesPersistedInstantOnReentry() {
    // A run re-entering the PENDING_CUTOVER handling after a crash must reuse the instant the prior
    // run persisted (1000), never re-capture a later one -- otherwise the repair floor would drift
    // past the real cutover and strand the post-cutover-window writes the partial pass re-verifies.
    assertEquals("A persisted cutover instant must be reused verbatim on re-entry", 1000L,
      TransformMonitorTask.resolveCutoverTs(recordWith(1000L, 5000L)));
  }

  @Test
  public void testResolveCutoverTsCapturesNowOnFirstRun() {
    // A first run (no persisted instant) captures the current time -- taken before the pointer
    // swap, this is the most conservative floor.
    ManualEnvironmentEdge edge = new ManualEnvironmentEdge();
    edge.setValue(4242L);
    EnvironmentEdgeManager.injectEdge(edge);
    try {
      assertEquals("A first run captures the current time as the cutover instant", 4242L,
        TransformMonitorTask.resolveCutoverTs(recordWith(null, null)));
    } finally {
      EnvironmentEdgeManager.reset();
    }
  }

  @Test
  public void testResultAlwaysBoundedAndPositiveAcrossDomain() {
    long[] samples = { Long.MIN_VALUE, -1L, 0L, 1L, 1000L, MIN_WAIT_MS, MAX_WAIT_MS / 2,
      MAX_WAIT_MS, MAX_WAIT_MS + 1, Long.MAX_VALUE / 2, Long.MAX_VALUE - 1, Long.MAX_VALUE };
    for (long f : samples) {
      long wait = TransformMonitorTask.boundedPartialPassWaitMs(f);
      assertTrue("wait must be >= floor for input " + f, wait >= MIN_WAIT_MS);
      assertTrue("wait must be <= ceiling for input " + f, wait <= MAX_WAIT_MS);
      // The deadline is currentTime + wait; a bounded positive wait cannot overflow it.
      assertTrue("wait must stay positive for input " + f, wait > 0);
    }
  }
}
