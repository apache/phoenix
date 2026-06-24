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

import static org.apache.phoenix.replication.reader.ReplicationLogReplayService.CONSISTENCY_POINT_UNAVAILABLE;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Tests for ReplicationLogReplayService.resolveConsistencyPoint caching behavior.
 */
public class ReplicationConsistencyPointTest {

  @Test
  public void testCachedConsistencyPointAvoidsRepeatedFetches() {
    Configuration conf = new Configuration(false);
    AtomicInteger fetchCount = new AtomicInteger(0);
    ReplicationLogReplayService.setConsistencyPointSupplierForTesting(conf, () -> {
      fetchCount.incrementAndGet();
      return 500000L;
    });

    try {
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

  @Test
  public void testTransientFailureNotCached_retriesOnNextCall() {
    Configuration conf = new Configuration(false);
    AtomicInteger fetchCount = new AtomicInteger(0);
    ReplicationLogReplayService.setConsistencyPointSupplierForTesting(conf, () -> {
      int attempt = fetchCount.incrementAndGet();
      if (attempt == 1) {
        throw new RuntimeException("Simulated transient failure");
      }
      return 700000L;
    });

    try {
      String table = "TEST_TABLE";
      String cf = "0";

      long result1 = ReplicationLogReplayService.resolveConsistencyPoint(conf, table, cf);
      assertEquals(CONSISTENCY_POINT_UNAVAILABLE, result1);

      long result2 = ReplicationLogReplayService.resolveConsistencyPoint(conf, table, cf);
      assertEquals(700000L, result2);

      assertEquals(2, fetchCount.get());
    } finally {
      ReplicationLogReplayService.resetInstanceForTesting();
    }
  }
}
