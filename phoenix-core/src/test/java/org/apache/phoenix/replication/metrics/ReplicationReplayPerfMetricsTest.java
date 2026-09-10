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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Unit tests for the replay performance metrics added under PHOENIX-7992. These verify the
 * metric-source plumbing (registration, recording, and read-back) for the six metrics across the
 * three replay metric sources:
 * <ul>
 * <li>{@link MetricsReplicationLogDiscoveryReplayImpl}: endToEndReplayLag, pickupLag (histograms)
 * and roundsExceedingRoundTime (counter)</li>
 * <li>{@link MetricsReplicationLogProcessorImpl}: successfulFileMutationsReplayedCount (counter)
 * and mutationsPerFile (histogram)</li>
 * <li>{@link MetricsReplicationLogTrackerReplayImpl}: markFileInProgressRenameFailedCount
 * (counter)</li>
 * </ul>
 * The sources are {@code static} and constructed once (mirroring
 * {@code ReplicationLogDiscoveryTest}) because Hadoop's {@code DefaultMetricsSystem} throws on
 * duplicate source registration and these sources do not all unregister cleanly on close(). Counter
 * assertions are therefore delta-based; each histogram is written by exactly one test method, so
 * its {@code getMax()} is deterministic.
 */
public class ReplicationReplayPerfMetricsTest {

  private static final String HA_GROUP = "replayPerfMetricsGroup";

  private static final MetricsReplicationLogDiscoveryReplayImpl DISCOVERY =
    new MetricsReplicationLogDiscoveryReplayImpl(HA_GROUP);
  private static final MetricsReplicationLogProcessorImpl PROCESSOR =
    new MetricsReplicationLogProcessorImpl(HA_GROUP);
  private static final MetricsReplicationLogTrackerReplayImpl TRACKER =
    new MetricsReplicationLogTrackerReplayImpl(HA_GROUP);

  /**
   * Metrics #1 (endToEndReplayLag), #2 (pickupLag), and #6 (roundsExceedingRoundTime) live on the
   * replay discovery source. The lag histograms report their max across samples; the slow-round
   * counter increments once per call.
   */
  @Test
  public void testDiscoveryReplayLagAndSlowRoundMetrics() {
    // #1 endToEndReplayLag histogram: getMax reflects the largest recorded sample.
    DISCOVERY.updateEndToEndReplayLag(1200L);
    DISCOVERY.updateEndToEndReplayLag(800L);
    assertEquals("endToEndReplayLag should report the max sample", 1200L,
      DISCOVERY.getEndToEndReplayLagHistogram().getMax());

    // #2 pickupLag histogram.
    DISCOVERY.updatePickupLag(300L);
    DISCOVERY.updatePickupLag(900L);
    assertEquals("pickupLag should report the max sample", 900L,
      DISCOVERY.getPickupLagHistogram().getMax());

    // #6 roundsExceedingRoundTime counter.
    long baseRounds = DISCOVERY.getCurrentMetricValues().getRoundsExceedingRoundTime();
    DISCOVERY.incrementRoundsExceedingRoundTime();
    DISCOVERY.incrementRoundsExceedingRoundTime();
    assertEquals("roundsExceedingRoundTime should increment once per call", baseRounds + 2,
      DISCOVERY.getCurrentMetricValues().getRoundsExceedingRoundTime());
  }

  /**
   * Metrics #3 (successfulFileMutationsReplayedCount) and #4 (mutationsPerFile) live on the log
   * processor source. The count accumulates the supplied delta; the per-file histogram reports its
   * max.
   */
  @Test
  public void testProcessorMutationMetrics() {
    // #3 successfulFileMutationsReplayedCount counter accumulates by the supplied delta.
    long baseMutations =
      PROCESSOR.getCurrentMetricValues().getSuccessfulFileMutationsReplayedCount();
    PROCESSOR.incrementSuccessfulFileMutationsReplayedCount(150L);
    PROCESSOR.incrementSuccessfulFileMutationsReplayedCount(50L);
    assertEquals("successfulFileMutationsReplayedCount should accumulate the supplied deltas",
      baseMutations + 200,
      PROCESSOR.getCurrentMetricValues().getSuccessfulFileMutationsReplayedCount());

    // #4 mutationsPerFile histogram reports the max per-file count.
    PROCESSOR.updateMutationsPerFile(150L);
    PROCESSOR.updateMutationsPerFile(400L);
    PROCESSOR.updateMutationsPerFile(30L);
    assertEquals("mutationsPerFile should report the max per-file count", 400L,
      PROCESSOR.getCurrentMetricValues().getMutationsPerFile());
  }

  /**
   * Metric #5 (markFileInProgressRenameFailedCount) lives on the tracker source and increments once
   * per call.
   */
  @Test
  public void testTrackerRenameFailedMetric() {
    long baseRenameFailures =
      TRACKER.getCurrentMetricValues().getMarkFileInProgressRenameFailedCount();
    TRACKER.incrementMarkFileInProgressRenameFailedCount();
    TRACKER.incrementMarkFileInProgressRenameFailedCount();
    TRACKER.incrementMarkFileInProgressRenameFailedCount();
    assertEquals("markFileInProgressRenameFailedCount should increment once per call",
      baseRenameFailures + 3,
      TRACKER.getCurrentMetricValues().getMarkFileInProgressRenameFailedCount());
  }
}
