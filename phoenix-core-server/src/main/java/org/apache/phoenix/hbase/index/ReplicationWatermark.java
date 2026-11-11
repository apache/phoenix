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

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * The replicated watermark of one region: a WAL sequence id W such that every HA edit of the region
 * with a sequence id at or below W is durable in a replication log. W may lag, but never runs ahead
 * of what is durable.
 * <p>
 * Each replicated batch registers a floor, the region's MVCC write point, before HBase assigns the
 * batch its sequence id (the MVCC write number is the WAL sequence id), so floor &lt; id. A batch
 * that fails before the WAL never registers. The floor is released once the batch is replicated or
 * has nothing to ship; otherwise it stays pinned until the region closes and holds W down. W is the
 * oldest floor still registered, or the write point when none is.
 */
final class ReplicationWatermark {

  /** A registered batch. Ordered by floor, then by a unique id, so equal floors coexist. */
  static final class Floor implements Comparable<Floor> {
    private final long floor;
    private final long id;

    private Floor(long floor, long id) {
      this.floor = floor;
      this.id = id;
    }

    @Override
    public int compareTo(Floor other) {
      int c = Long.compare(floor, other.floor);
      return c != 0 ? c : Long.compare(id, other.id);
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof Floor && compareTo((Floor) o) == 0;
    }

    @Override
    public int hashCode() {
      return Long.hashCode(floor) * 31 + Long.hashCode(id);
    }
  }

  private final ConcurrentSkipListMap<Floor, Boolean> inFlight = new ConcurrentSkipListMap<>();
  private final AtomicLong ids = new AtomicLong();

  /**
   * Registers a batch. Must be called before the batch's sequence id is assigned, with the region's
   * current write point.
   */
  Floor register(long writePoint) {
    Floor floor = new Floor(writePoint, ids.incrementAndGet());
    inFlight.put(floor, Boolean.TRUE);
    return floor;
  }

  /**
   * W for a batch that has registered and not been released: the oldest floor. The caller's own
   * floor keeps the map non-empty, and every floor is a past write point, so no write point read is
   * needed. The result is at most the caller's floor, so below its own sequence id.
   */
  long watermarkForRegistered() {
    return inFlight.firstEntry().getKey().floor;
  }

  /**
   * W for a caller that has not registered: the map may be empty, so the write point is the
   * fallback. The write point must be read before the floors: a batch not yet in the map then
   * registers a floor at or above it, and gets an id above it.
   */
  long watermarkUnregistered(LongSupplier writePoint) {
    long wp = writePoint.getAsLong();
    Map.Entry<Floor, Boolean> oldest = inFlight.firstEntry();
    return oldest == null ? wp : Math.min(wp, oldest.getKey().floor);
  }

  /** Releases a batch that is replicated or had nothing to ship. */
  void release(Floor floor) {
    inFlight.remove(floor);
  }
}
