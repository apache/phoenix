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
package org.apache.phoenix.replication.reader;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Tests for the replication compaction guard row-level cap logic. Verifies the contract:
 * maxLookbackWindowStartForRow = min(max(ttlWindowStart, maxLookbackWindowStart), consistencyPoint)
 */
public class ReplicationCompactionGuardTest {

  /**
   * Simulates the RowContext computation in CompactionScanner.RowContext.setTTL():
   * maxLookbackWindowStartForRow = min(max(ttlWindowStart, maxLookbackWindowStart),
   * replicationConsistencyPoint)
   */
  private static long computeRowBoundary(long ttlWindowStart, long maxLookbackWindowStart,
    long replicationConsistencyPoint) {
    long rowBoundary = Math.max(ttlWindowStart, maxLookbackWindowStart);
    return Math.min(rowBoundary, replicationConsistencyPoint);
  }

  @Test
  public void testTtlHigherThanConsistencyPoint_capApplied() {
    long now = System.currentTimeMillis();
    long maxLookbackWindowStart = now - 86400000L;
    long ttlWindowStart = now - 3600000L;
    long consistencyPoint = now - 7200000L;

    long result = computeRowBoundary(ttlWindowStart, maxLookbackWindowStart, consistencyPoint);

    assertEquals(consistencyPoint, result);
  }

  @Test
  public void testTtlLowerThanConsistencyPoint_noCapNeeded() {
    long now = System.currentTimeMillis();
    long maxLookbackWindowStart = now - 86400000L;
    long ttlWindowStart = now - 14400000L;
    long consistencyPoint = now - 3600000L;

    long result = computeRowBoundary(ttlWindowStart, maxLookbackWindowStart, consistencyPoint);

    assertEquals(ttlWindowStart, result);
  }

  @Test
  public void testNoTtl_capAppliedOnMaxLookback() {
    long now = System.currentTimeMillis();
    long maxLookbackWindowStart = now - 86400000L;
    long ttlWindowStart = 1L;
    long consistencyPoint = now - 172800000L;

    long result = computeRowBoundary(ttlWindowStart, maxLookbackWindowStart, consistencyPoint);

    assertEquals(consistencyPoint, result);
  }

  @Test
  public void testNoTtl_consistencyPointAheadOfMaxLookback() {
    long now = System.currentTimeMillis();
    long maxLookbackWindowStart = now - 86400000L;
    long ttlWindowStart = 1L;
    long consistencyPoint = now - 60000L;

    long result = computeRowBoundary(ttlWindowStart, maxLookbackWindowStart, consistencyPoint);

    assertEquals(maxLookbackWindowStart, result);
  }

  @Test
  public void testConsistencyPointZero_retainsAll() {
    long now = System.currentTimeMillis();
    long maxLookbackWindowStart = now - 86400000L;
    long ttlWindowStart = now - 3600000L;
    long consistencyPoint = 0L;

    long result = computeRowBoundary(ttlWindowStart, maxLookbackWindowStart, consistencyPoint);

    assertEquals(0L, result);
  }

  @Test
  public void testGuardDisabled_longMaxValueNoOp() {
    long now = System.currentTimeMillis();
    long maxLookbackWindowStart = now - 86400000L;
    long ttlWindowStart = now - 3600000L;
    long consistencyPoint = Long.MAX_VALUE;

    long result = computeRowBoundary(ttlWindowStart, maxLookbackWindowStart, consistencyPoint);

    assertEquals(ttlWindowStart, result);
  }

  @Test
  public void testCachedConsistencyPointAvoidsRepeatedFetches() {
    AtomicInteger fetchCount = new AtomicInteger(0);
    ReplicationLogReplayService.setConsistencyPointSupplierForTesting(() -> {
      fetchCount.incrementAndGet();
      return 500000L;
    });

    try {
      Configuration conf = new Configuration(false);
      String table = "TEST_TABLE";
      String cf = "0";

      long result1 = ReplicationLogReplayService.resolveConsistencyPoint(conf, table, cf);
      long result2 = ReplicationLogReplayService.resolveConsistencyPoint(conf, table, cf);
      long result3 = ReplicationLogReplayService.resolveConsistencyPoint(conf, table, cf);

      assertEquals(500000L, result1);
      assertEquals(500000L, result2);
      assertEquals(500000L, result3);
      assertEquals(1, fetchCount.get());
    } finally {
      ReplicationLogReplayService.resetInstanceForTesting();
    }
  }
}
