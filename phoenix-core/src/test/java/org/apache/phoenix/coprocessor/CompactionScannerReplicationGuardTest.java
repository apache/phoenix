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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Tests for the replication consistency point guard in CompactionScanner. Tests the pure adjustment
 * logic (adjustMaxLookbackWindowStart) which is the core of the guard — ensuring
 * maxLookbackWindowStart is floored to the consistency point.
 */
public class CompactionScannerReplicationGuardTest {

  private static final String TABLE_NAME = "TEST_TABLE";
  private static final String CF_NAME = "0";

  @Test
  public void testAdjustsWindowWhenConsistencyPointIsLower() {
    long maxLookbackWindowStart = 1000000L;
    long consistencyPoint = 500000L;

    long result = CompactionScanner.adjustMaxLookbackWindowStart(maxLookbackWindowStart,
      consistencyPoint, TABLE_NAME, CF_NAME);

    assertEquals(consistencyPoint, result);
  }

  @Test
  public void testNoChangeWhenConsistencyPointIsHigher() {
    long maxLookbackWindowStart = 500000L;
    long consistencyPoint = 1000000L;

    long result = CompactionScanner.adjustMaxLookbackWindowStart(maxLookbackWindowStart,
      consistencyPoint, TABLE_NAME, CF_NAME);

    assertEquals(maxLookbackWindowStart, result);
  }

  @Test
  public void testNoChangeWhenConsistencyPointEqualsWindowStart() {
    long maxLookbackWindowStart = 500000L;
    long consistencyPoint = 500000L;

    long result = CompactionScanner.adjustMaxLookbackWindowStart(maxLookbackWindowStart,
      consistencyPoint, TABLE_NAME, CF_NAME);

    assertEquals(maxLookbackWindowStart, result);
  }

  @Test
  public void testConsistencyPointAtZeroRetainsAll() {
    long maxLookbackWindowStart = 1000000L;
    long consistencyPoint = 0L;

    long result = CompactionScanner.adjustMaxLookbackWindowStart(maxLookbackWindowStart,
      consistencyPoint, TABLE_NAME, CF_NAME);

    assertEquals(0L, result);
  }

  @Test
  public void testLargeTimestampsNoAdjustmentNeeded() {
    long maxLookbackWindowStart = System.currentTimeMillis() - 86400000L;
    long consistencyPoint = System.currentTimeMillis() - 120000L;

    long result = CompactionScanner.adjustMaxLookbackWindowStart(maxLookbackWindowStart,
      consistencyPoint, TABLE_NAME, CF_NAME);

    assertEquals(maxLookbackWindowStart, result);
  }

  @Test
  public void testConsistencyPointFarInPastPushesWindowBack() {
    long maxLookbackWindowStart = System.currentTimeMillis() - 86400000L;
    long consistencyPoint = System.currentTimeMillis() - 604800000L;

    long result = CompactionScanner.adjustMaxLookbackWindowStart(maxLookbackWindowStart,
      consistencyPoint, TABLE_NAME, CF_NAME);

    assertEquals(consistencyPoint, result);
  }
}
