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
package org.apache.phoenix.hbase.index;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class IndexCDCConsumerProgressTest {

  private static final long BUFFER_MS = 5_000L;
  private static final long START_TIME = 1_000_000L;

  private IndexCDCConsumerProgress newProgress() {
    return new IndexCDCConsumerProgress(START_TIME, BUFFER_MS);
  }

  @Test
  public void coldStartReportsLagSinceConsumerStart() {
    IndexCDCConsumerProgress p = newProgress();
    assertEquals(100L, p.currentLagMs(START_TIME + 100L));
    assertEquals(0L, p.getEffectiveWatermark());
  }

  @Test
  public void processedAdvancesWatermark() {
    IndexCDCConsumerProgress p = newProgress();
    long processed = START_TIME + 10_000L;
    p.recordProcessed(processed);
    assertEquals(processed, p.getEffectiveWatermark());
    assertEquals(2_000L, p.currentLagMs(processed + 2_000L));
  }

  @Test
  public void processedIsMonotonic() {
    IndexCDCConsumerProgress p = newProgress();
    p.recordProcessed(START_TIME + 1_000L);
    p.recordProcessed(START_TIME + 500L);
    assertEquals(START_TIME + 1_000L, p.getEffectiveWatermark());
  }

  @Test
  public void emptyPollAdvancesWatermarkBuffersBelowPollTime() {
    IndexCDCConsumerProgress p = newProgress();
    long pollEnd = START_TIME + 20_000L;
    p.recordEmptyPoll(pollEnd);
    assertEquals(pollEnd - BUFFER_MS, p.getEffectiveWatermark());
    // Lag at the same instant collapses to the buffer baseline.
    assertEquals(BUFFER_MS, p.currentLagMs(pollEnd));
  }

  @Test
  public void emptyPollIsMonotonic() {
    IndexCDCConsumerProgress p = newProgress();
    long firstPoll = START_TIME + 20_000L;
    long earlierPoll = START_TIME + 10_000L;
    p.recordEmptyPoll(firstPoll);
    p.recordEmptyPoll(earlierPoll);
    assertEquals(firstPoll, p.getLastEmptyPollEndTime());
    assertEquals(firstPoll - BUFFER_MS, p.getEffectiveWatermark());
  }

  @Test
  public void watermarkIsMaxOfProcessedAndEmptyPollFloor() {
    IndexCDCConsumerProgress p = newProgress();
    long processed = START_TIME + 50_000L;
    long pollEnd = START_TIME + 30_000L;
    p.recordProcessed(processed);
    p.recordEmptyPoll(pollEnd);
    // processed dominates because (pollEnd - BUFFER_MS) < processed
    assertEquals(processed, p.getEffectiveWatermark());

    // A later empty poll above (processed + BUFFER_MS) advances the watermark again.
    long laterPoll = processed + BUFFER_MS + 1_000L;
    p.recordEmptyPoll(laterPoll);
    assertEquals(laterPoll - BUFFER_MS, p.getEffectiveWatermark());
  }

  @Test
  public void idleAfterEmptyPollStaysBoundedByBufferPlusElapsed() {
    IndexCDCConsumerProgress p = newProgress();
    long pollEnd = START_TIME + 20_000L;
    p.recordEmptyPoll(pollEnd);
    long elapsed = 7_500L;
    assertEquals(BUFFER_MS + elapsed, p.currentLagMs(pollEnd + elapsed));
  }

  @Test
  public void negativeLagClampedToZero() {
    IndexCDCConsumerProgress p = newProgress();
    long processed = START_TIME + 50_000L;
    p.recordProcessed(processed);
    // clock went backwards relative to the watermark
    assertEquals(0L, p.currentLagMs(processed - 100L));
  }

  @Test
  public void emptyPollOlderThanBufferContributesNothing() {
    IndexCDCConsumerProgress p = newProgress();
    long pollEnd = BUFFER_MS - 1L;
    p.recordEmptyPoll(pollEnd);
    // pollEnd - BUFFER_MS would be negative; do not pollute the watermark.
    assertEquals(pollEnd, p.getLastEmptyPollEndTime());
    assertEquals(0L, p.getEffectiveWatermark());
  }
}
