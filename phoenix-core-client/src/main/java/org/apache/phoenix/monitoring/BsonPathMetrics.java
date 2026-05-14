/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.monitoring;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Lightweight counters for BSON-path index activity. Best-effort, client-process-local.
 * Counters are static so they aggregate across all connections in this JVM.
 */
public final class BsonPathMetrics {

  private static final AtomicLong SPARSE_SKIPS = new AtomicLong();
  private static final AtomicLong REWRITE_HITS = new AtomicLong();
  private static final AtomicLong REWRITE_MISSES = new AtomicLong();

  private BsonPathMetrics() {}

  public static void incrementSparseSkips() { SPARSE_SKIPS.incrementAndGet(); }
  public static void incrementRewriteHits() { REWRITE_HITS.incrementAndGet(); }
  public static void incrementRewriteMisses() { REWRITE_MISSES.incrementAndGet(); }

  public static long getSparseSkips() { return SPARSE_SKIPS.get(); }
  public static long getRewriteHits() { return REWRITE_HITS.get(); }
  public static long getRewriteMisses() { return REWRITE_MISSES.get(); }

  /** Reset all counters; for use in tests only. */
  public static void resetForTest() {
    SPARSE_SKIPS.set(0);
    REWRITE_HITS.set(0);
    REWRITE_MISSES.set(0);
  }
}
