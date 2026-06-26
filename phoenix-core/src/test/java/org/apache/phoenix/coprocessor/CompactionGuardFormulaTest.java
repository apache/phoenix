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
package org.apache.phoenix.coprocessor;

import static org.apache.phoenix.replication.reader.ReplicationLogReplayService.CONSISTENCY_POINT_GUARD_DISABLED;
import static org.apache.phoenix.replication.reader.ReplicationLogReplayService.CONSISTENCY_POINT_UNAVAILABLE;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Unit tests for CompactionScanner.computeRowMaxLookbackWithGuard formula. Covers scenarios
 * including those unreachable via standard TTL floor enforcement to guard against future changes to
 * the TTL computation.
 */
public class CompactionGuardFormulaTest {

  @Test
  public void testTtlDominatedGuardCaps() {
    // ttlWindowStart > maxLookbackWindowStart > consistencyPoint
    // Simulates conditional TTL or future removal of TTL floor enforcement
    long consistencyPoint = 1000L;
    long maxLookbackWindowStart = 2000L;
    long ttlWindowStart = 3000L;

    long result = CompactionScanner.computeRowMaxLookbackWithGuard(ttlWindowStart,
      maxLookbackWindowStart, consistencyPoint);

    // max(3000, 2000) = 3000, min(3000, 999) = 999 → guard caps at consistencyPoint - 1
    assertEquals(consistencyPoint - 1, result);
  }

  @Test
  public void testLookbackDominatedGuardCaps() {
    // maxLookbackWindowStart > ttlWindowStart > consistencyPoint
    long consistencyPoint = 1000L;
    long ttlWindowStart = 2000L;
    long maxLookbackWindowStart = 3000L;

    long result = CompactionScanner.computeRowMaxLookbackWithGuard(ttlWindowStart,
      maxLookbackWindowStart, consistencyPoint);

    // max(2000, 3000) = 3000, min(3000, 999) = 999 → guard caps at consistencyPoint - 1
    assertEquals(consistencyPoint - 1, result);
  }

  @Test
  public void testConsistencyPointBeyondBoth_guardInactive() {
    // consistencyPoint > max(ttlWindowStart, maxLookbackWindowStart)
    long maxLookbackWindowStart = 2000L;
    long ttlWindowStart = 1000L;
    long consistencyPoint = 5000L;

    long result = CompactionScanner.computeRowMaxLookbackWithGuard(ttlWindowStart,
      maxLookbackWindowStart, consistencyPoint);

    // max(1000, 2000) = 2000, min(2000, 4999) = 2000 → guard doesn't restrict
    assertEquals(maxLookbackWindowStart, result);
  }

  @Test
  public void testConsistencyPointZero_retainsAll() {
    // consistencyPoint = UNAVAILABLE signals fallback — retain all delete markers
    long maxLookbackWindowStart = 2000L;
    long ttlWindowStart = 3000L;
    long consistencyPoint = CONSISTENCY_POINT_UNAVAILABLE;

    long result = CompactionScanner.computeRowMaxLookbackWithGuard(ttlWindowStart,
      maxLookbackWindowStart, consistencyPoint);

    assertEquals(CONSISTENCY_POINT_UNAVAILABLE, result);
  }

  @Test
  public void testConsistencyPointMaxValue_guardDisabled() {
    // GUARD_DISABLED used when replay is off — guard is effectively a no-op
    long maxLookbackWindowStart = 2000L;
    long ttlWindowStart = 3000L;
    long consistencyPoint = CONSISTENCY_POINT_GUARD_DISABLED;

    long result = CompactionScanner.computeRowMaxLookbackWithGuard(ttlWindowStart,
      maxLookbackWindowStart, consistencyPoint);

    // max(3000, 2000) = 3000, min(3000, MAX_VALUE) = 3000 → normal behavior
    assertEquals(ttlWindowStart, result);
  }

  @Test
  public void testBoundaryDeleteAtExactlyConsistencyPoint_isRetained() {
    // A delete marker at ts == consistencyPoint has NOT been replayed (exclusive upper bound).
    // The formula must produce a boundary below consistencyPoint so that the strict-greater
    // retention check (ts > boundary) retains it.
    long consistencyPoint = 5000L;
    long maxLookbackWindowStart = 6000L;
    long ttlWindowStart = 4000L;

    long result = CompactionScanner.computeRowMaxLookbackWithGuard(ttlWindowStart,
      maxLookbackWindowStart, consistencyPoint);

    // Boundary = consistencyPoint - 1 = 4999; a cell at ts=5000 satisfies ts > 4999
    assertEquals(consistencyPoint - 1, result);
  }

  @Test
  public void testConsistencyPointBetweenInputs() {
    // ttlWindowStart < consistencyPoint < maxLookbackWindowStart
    long ttlWindowStart = 1000L;
    long consistencyPoint = 2500L;
    long maxLookbackWindowStart = 3000L;

    long result = CompactionScanner.computeRowMaxLookbackWithGuard(ttlWindowStart,
      maxLookbackWindowStart, consistencyPoint);

    // max(1000, 3000) = 3000, min(3000, 2499) = 2499 → guard caps
    assertEquals(consistencyPoint - 1, result);
  }
}
