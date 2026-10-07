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
package org.apache.phoenix.hbase.index.hnsw;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Manages RegionServer level off-heap memory allocations for materialized HNSW graph segments,
 * bounded by {@value QueryServices#HNSW_OFFHEAP_MAX_BYTES_ATTRIB}. Allocations that exceed the
 * configured memory budget trigger LRU eviction of resident segments.
 */
public final class HnswOffheapAllocator {
  private static final Logger LOG = LoggerFactory.getLogger(HnswOffheapAllocator.class);

  private static volatile HnswOffheapAllocator instance;

  private final long maxBytes;
  private long allocatedBytes;
  // Track allocations in access order for LRU eviction
  private final LinkedHashMap<Object, Allocation> allocations =
    new LinkedHashMap<>(16, 0.75f, true);

  private static final class Allocation {
    final long size;
    final Runnable onEvict;

    Allocation(long size, Runnable onEvict) {
      this.size = size;
      this.onEvict = onEvict;
    }
  }

  /** Returns the singleton allocator instance, initialized from the provided configuration. */
  public static HnswOffheapAllocator get(Configuration conf) {
    if (instance == null) {
      synchronized (HnswOffheapAllocator.class) {
        if (instance == null) {
          instance =
            new HnswOffheapAllocator(conf.getLong(QueryServices.HNSW_OFFHEAP_MAX_BYTES_ATTRIB,
              QueryServicesOptions.DEFAULT_HNSW_OFFHEAP_MAX_BYTES));
        }
      }
    }
    return instance;
  }

  public HnswOffheapAllocator(long maxBytes) {
    this.maxBytes = maxBytes;
  }

  /**
   * Allocates a direct byte buffer of the requested size for the specified owner, evicting
   * least-recently-used allocations as necessary to remain within the configured memory budget.
   * @param owner   allocation owner identifier
   * @param size    buffer size in bytes
   * @param onEvict callback invoked if this allocation is subsequently evicted
   * @return allocated direct byte buffer
   * @throws IOException if the requested size exceeds the total configured budget
   */
  public ByteBuffer allocate(Object owner, int size, Runnable onEvict) throws IOException {
    if (size > maxBytes) {
      throw new IOException("HNSW segment of " + size + " bytes exceeds the off-heap budget of "
        + maxBytes + " bytes (" + QueryServices.HNSW_OFFHEAP_MAX_BYTES_ATTRIB + ")");
    }
    List<Runnable> evicted = new ArrayList<>();
    synchronized (this) {
      release(owner);
      Iterator<Map.Entry<Object, Allocation>> lru = allocations.entrySet().iterator();
      while (allocatedBytes + size > maxBytes && lru.hasNext()) {
        Allocation victim = lru.next().getValue();
        lru.remove();
        allocatedBytes -= victim.size;
        evicted.add(victim.onEvict);
      }
      allocations.put(owner, new Allocation(size, onEvict));
      allocatedBytes += size;
    }
    if (!evicted.isEmpty()) {
      LOG.info("Evicted {} HNSW segments to allocate {} bytes", evicted.size(), size);
    }
    for (Runnable onEvictVictim : evicted) {
      onEvictVictim.run();
    }
    return ByteBuffer.allocateDirect(size);
  }

  /** Updates access ordering for the specified allocation owner. */
  public synchronized void touch(Object owner) {
    allocations.get(owner);
  }

  /** Releases the allocation owned by the specified identifier. */
  public synchronized void release(Object owner) {
    Allocation a = allocations.remove(owner);
    if (a != null) {
      allocatedBytes -= a.size;
    }
  }

  /** Evicts every resident segment, as memory pressure would. */
  @VisibleForTesting
  public void evictAll() {
    List<Runnable> evicted = new ArrayList<>();
    synchronized (this) {
      for (Allocation a : allocations.values()) {
        evicted.add(a.onEvict);
      }
      allocations.clear();
      allocatedBytes = 0;
    }
    for (Runnable onEvict : evicted) {
      onEvict.run();
    }
  }

  /** Returns the total number of bytes currently allocated. */
  public synchronized long getAllocatedBytes() {
    return allocatedBytes;
  }
}
