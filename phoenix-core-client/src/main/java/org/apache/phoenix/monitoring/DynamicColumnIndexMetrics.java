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
 * Lightweight client-side counters for dynamic-column secondary indexes.
 * Mirrors BsonPathMetrics. Counters are best-effort, JVM-local, and
 * intended for ad-hoc diagnostics — not aggregated metric reporting.
 */
public final class DynamicColumnIndexMetrics {

  private static final AtomicLong PROMOTIONS = new AtomicLong();
  private static final AtomicLong UNPROMOTIONS = new AtomicLong();
  private static final AtomicLong TYPE_CONFLICT_REJECTS = new AtomicLong();

  private DynamicColumnIndexMetrics() {}

  public static void incrementPromotions() { PROMOTIONS.incrementAndGet(); }
  public static void incrementUnpromotions() { UNPROMOTIONS.incrementAndGet(); }
  public static void incrementTypeConflictRejects() { TYPE_CONFLICT_REJECTS.incrementAndGet(); }

  public static long getPromotions() { return PROMOTIONS.get(); }
  public static long getUnpromotions() { return UNPROMOTIONS.get(); }
  public static long getTypeConflictRejects() { return TYPE_CONFLICT_REJECTS.get(); }

  /** Test-only. Not part of the public API. */
  public static void resetForTesting() {
    PROMOTIONS.set(0);
    UNPROMOTIONS.set(0);
    TYPE_CONFLICT_REJECTS.set(0);
  }
}
