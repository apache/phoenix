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
package org.apache.phoenix.replication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ReplicationLogTest {

  @Test
  public void testComputeInitialDelay() {
    long rotationTimeMs = 60_000L;

    // Exactly on a round boundary: full round until next tick
    long now = 120_000L;
    long roundStart = 120_000L;
    assertEquals(rotationTimeMs,
      ReplicationLog.computeInitialDelay(now, roundStart, rotationTimeMs));

    // Middle of the round: half a round remaining
    now = 150_000L;
    roundStart = 120_000L;
    assertEquals(30_000L, ReplicationLog.computeInitialDelay(now, roundStart, rotationTimeMs));

    // Near the end of a round: small but positive delay
    now = 179_999L;
    roundStart = 120_000L;
    assertEquals(1L, ReplicationLog.computeInitialDelay(now, roundStart, rotationTimeMs));

    // Just after a round boundary
    now = 120_001L;
    roundStart = 120_000L;
    assertEquals(59_999L, ReplicationLog.computeInitialDelay(now, roundStart, rotationTimeMs));
  }

  @Test
  public void testComputeInitialDelayIsAlwaysPositive() {
    long rotationTimeMs = 60_000L;
    long dayStart = 1704067200000L;

    for (int offsetMs = 0; offsetMs < 60_000; offsetMs += 1000) {
      long now = dayStart + offsetMs;
      long roundStart = (now / rotationTimeMs) * rotationTimeMs;
      long delay = ReplicationLog.computeInitialDelay(now, roundStart, rotationTimeMs);
      assertTrue("Delay must be > 0 for offset " + offsetMs, delay > 0);
      assertTrue("Delay must be <= rotationTimeMs for offset " + offsetMs, delay <= rotationTimeMs);
    }
  }
}
