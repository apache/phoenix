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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link MetricsReplicationLogGroupSourceImpl}, focused on verifying that the
 * histogram unit conventions documented in {@link MetricsReplicationLogGroupSource} are honored:
 * appendTime and pendingSyncWaitTime record nanoseconds verbatim; syncTime, fsSyncTime,
 * ringBufferTime, and rotationTime convert input nanoseconds to milliseconds before recording.
 */
public class MetricsReplicationLogGroupSourceImplTest {

  private MetricsReplicationLogGroupSourceImpl source;

  @Before
  public void setUp() {
    source = new MetricsReplicationLogGroupSourceImpl("testHaGroup");
  }

  @After
  public void tearDown() {
    source.close();
  }

  @Test
  public void testNanosecondHistogramsRecordInputVerbatim() {
    source.updateAppendTime(1500L);
    assertEquals(1500L, source.getCurrentMetricValues().getAppendTimeMax());

    source.updatePendingSyncWaitTime(2_500_000L);
    assertEquals(2_500_000L, source.getCurrentMetricValues().getPendingSyncWaitTimeMax());

    source.updateRingBufferTime(7_500_000L);
    assertEquals(7_500_000L, source.getCurrentMetricValues().getRingBufferTimeMax());
  }

  @Test
  public void testMillisecondHistogramsConvertNsToMs() {
    source.updateSyncTime(5_000_000L);
    assertEquals(5L, source.getCurrentMetricValues().getSyncTimeMax());

    source.updateFsSyncTime(1_000_000L);
    assertEquals(1L, source.getCurrentMetricValues().getFsSyncTimeMax());

    source.updateRotationTime(35_000_000L);
    assertEquals(35L, source.getCurrentMetricValues().getRotationTimeMax());
  }

  @Test
  public void testSubMillisecondInputTruncatesToZero() {
    source.updateSyncTime(500_000L);
    assertEquals(0L, source.getCurrentMetricValues().getSyncTimeMax());
  }

  @Test
  public void testHistogramReportsMaxAcrossSamples() {
    source.updateSyncTime(2_000_000L);
    source.updateSyncTime(10_000_000L);
    source.updateSyncTime(5_000_000L);
    assertEquals(10L, source.getCurrentMetricValues().getSyncTimeMax());
  }

  @Test
  public void testCounters() {
    source.incrementRotationCount();
    source.incrementRotationCount();
    source.incrementRotationFailureCount();
    source.incrementSyncToSafTransitions();

    ReplicationLogMetricValues v = source.getCurrentMetricValues();
    assertEquals(2L, v.getRotationCount());
    assertEquals(1L, v.getRotationFailuresCount());
    assertEquals(1L, v.getSyncToSafTransitions());
  }

  @Test
  public void testBatchSizeAndPendingSyncCount() {
    source.updateBatchSize(100L);
    source.updatePendingSyncCount(50L);
    ReplicationLogMetricValues v = source.getCurrentMetricValues();
    assertEquals(100L, v.getBatchSizeMax());
    assertEquals(50L, v.getPendingSyncCountMax());
  }
}
