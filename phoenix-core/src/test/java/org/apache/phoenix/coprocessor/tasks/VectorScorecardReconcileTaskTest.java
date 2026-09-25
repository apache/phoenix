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

import static org.apache.phoenix.coprocessor.tasks.VectorScorecardReconcileTask.isDue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.phoenix.index.vector.GenerationSummary;
import org.junit.Test;

public class VectorScorecardReconcileTaskTest {

  private static final long INTERVAL_MS = 86400000L;
  private static final long NOW = 1_000_000_000_000L;

  private static GenerationSummary summaryLastReconciledAt(Long lastScorecardUpdate) {
    return new GenerationSummary.Builder().setIndexName("MY_IDX").setGenerationId(1L)
      .setLastScorecardUpdate(lastScorecardUpdate).build();
  }

  /** Tests that a generation that has never been reconciled is due immediately. */
  @Test
  public void testNeverReconciledIsDueImmediately() {
    assertTrue("a generation with no summary row has never been reconciled",
      isDue(null, INTERVAL_MS, NOW));
    assertTrue("a summary that has never recorded a reconciliation is due",
      isDue(summaryLastReconciledAt(null), INTERVAL_MS, NOW));
  }

  @Test
  public void testNotDueUntilIntervalElapses() {
    assertFalse("just reconciled", isDue(summaryLastReconciledAt(NOW), INTERVAL_MS, NOW));
    assertFalse("one millisecond short of the interval",
      isDue(summaryLastReconciledAt(NOW - INTERVAL_MS + 1), INTERVAL_MS, NOW));
  }

  @Test
  public void testDueOnceIntervalElapses() {
    assertTrue("exactly at the interval",
      isDue(summaryLastReconciledAt(NOW - INTERVAL_MS), INTERVAL_MS, NOW));
    assertTrue("well past the interval",
      isDue(summaryLastReconciledAt(NOW - 10 * INTERVAL_MS), INTERVAL_MS, NOW));
  }

  /** Tests that an interval of zero is always due. */
  @Test
  public void testZeroIntervalIsAlwaysDue() {
    assertTrue(isDue(summaryLastReconciledAt(NOW), 0L, NOW));
  }

  /** Tests that a timestamp in the future is not treated as due. */
  @Test
  public void testTimestampInTheFutureIsNotDue() {
    assertFalse(isDue(summaryLastReconciledAt(NOW + INTERVAL_MS), INTERVAL_MS, NOW));
  }
}
