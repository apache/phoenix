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
import java.lang.management.MemoryType;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.util.DirectMemoryUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.UnsafeAccess;
import org.apache.phoenix.hbase.index.metrics.MetricsIndexerSourceFactory;
import org.apache.phoenix.hbase.index.metrics.MetricsVectorIndexSource;
import org.apache.phoenix.hbase.index.metrics.MetricsVectorIndexSource.HnswMetric;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Controls the off-heap memory of the loaded HNSW graph segments of one JVM. A RegionServer has one
 * allocator, and a client JVM has one allocator. The budget
 * {@value QueryServices#HNSW_OFFHEAP_MAX_BYTES_ATTRIB} limits each allocator. If a new allocation
 * does not fit in the budget, the allocator evicts the least recently used resident segments.
 * <p>
 * The budget counts a {@link Block} from its allocation until the last release frees its buffer.
 * The caller holds a reference while it fills or reads the buffer. The allocator holds a reference
 * while the block is resident. Eviction drops the reference of the allocator, and the last release
 * frees the direct buffer immediately. A search that reads an evicted block keeps the block in the
 * budget until the search ends. For that time, the counted memory can be more than the budget.
 * <p>
 * Eviction cannot remove a block that is not resident. A block is not resident before its load
 * commits it, and after its eviction while a search still reads it. If these blocks hold too much
 * of the budget, eviction cannot make enough space. Then the allocation fails with a retryable
 * {@link IOException}.
 * <p>
 * The JVM must allow the budget as direct memory. In a RegionServer, the budget is at most half of
 * the direct memory that an off-heap BucketCache and an off-heap memstore do not use. This keeps
 * direct memory available for the RPC buffers of HBase. In other JVMs, such as the fsck tools, the
 * budget is at most half of the direct memory.
 * <p>
 * Only the allocator of a RegionServer publishes metrics. It updates them in the locks that it
 * already holds. The allocator of another JVM publishes nothing, so that the metrics of a server
 * show only the segments of that server.
 */
public final class HnswOffheapAllocator {
  private static final Logger LOG = LoggerFactory.getLogger(HnswOffheapAllocator.class);

  private static volatile HnswOffheapAllocator instance;
  private static HnswOffheapAllocator clientInstance;

  private final long maxBytes;
  private long allocatedBytes;
  // The metrics source of the server, or null for an allocator that publishes no metrics
  private final MetricsVectorIndexSource metrics;
  // The resident blocks by owner. The access order gives the LRU eviction order.
  private final LinkedHashMap<Object, Block> resident = new LinkedHashMap<>(16, 0.75f, true);

  /** A direct buffer that the budget counts until the release of its last reference. */
  final class Block {
    private final ByteBuffer buffer;
    private final AtomicInteger refs = new AtomicInteger(1);
    // The eviction callback. The allocator sets it when the block becomes resident, under the
    // allocator lock.
    private Runnable onEvict;

    private Block(ByteBuffer buffer) {
      this.buffer = buffer;
    }

    ByteBuffer buffer() {
      return buffer;
    }

    /** Takes a reference. Returns false if the last release already freed the buffer. */
    boolean retain() {
      for (;;) {
        int r = refs.get();
        if (r == 0) {
          return false;
        }
        if (refs.compareAndSet(r, r + 1)) {
          return true;
        }
      }
    }

    /**
     * Drops a reference. The last release frees the buffer and gives its size back to the budget.
     */
    void release() {
      if (refs.decrementAndGet() == 0) {
        UnsafeAccess.freeDirectBuffer(buffer);
        discharge(buffer.capacity());
      }
    }
  }

  /** Returns the allocator of this RegionServer. The RegionServer configuration sets its budget. */
  public static HnswOffheapAllocator get(Configuration conf) {
    if (instance == null) {
      synchronized (HnswOffheapAllocator.class) {
        if (instance == null) {
          instance =
            new HnswOffheapAllocator(budget(conf, DirectMemoryUtils.getDirectMemorySize(), true),
              MetricsIndexerSourceFactory.getInstance().getMetricsVectorIndexSource());
        }
      }
    }
    return instance;
  }

  /**
   * Returns the allocator of a JVM that hosts no RegionServer, such as an fsck tool or a mapper.
   * The RegionServer memory settings in its configuration do not apply to this JVM.
   */
  public static synchronized HnswOffheapAllocator getForClient(Configuration conf) {
    if (clientInstance == null) {
      clientInstance =
        new HnswOffheapAllocator(budget(conf, DirectMemoryUtils.getDirectMemorySize(), false));
    }
    return clientInstance;
  }

  /**
   * Returns the configured budget, but not more than half of the direct memory that the JVM allows.
   * In a RegionServer, an off-heap BucketCache and an off-heap memstore use part of the direct
   * memory, so that part is not available.
   */
  static long budget(Configuration conf, long maxDirectMemory, boolean regionServer) {
    long budget = conf.getLong(QueryServices.HNSW_OFFHEAP_MAX_BYTES_ATTRIB,
      QueryServicesOptions.DEFAULT_HNSW_OFFHEAP_MAX_BYTES);
    long available = maxDirectMemory;
    if (regionServer) {
      if (conf.get(HConstants.BUCKET_CACHE_IOENGINE_KEY, "").startsWith("offheap")) {
        // The BucketCache size is in megabytes
        available -= (long) (conf.getFloat(HConstants.BUCKET_CACHE_SIZE_KEY, 0F) * 1024 * 1024);
      }
      Pair<Long, MemoryType> memstore = MemorySizeUtil.getGlobalMemStoreSize(conf);
      if (memstore.getSecond() == MemoryType.NON_HEAP) {
        available -= memstore.getFirst();
      }
    }
    long limit = Math.max(0, available / 2);
    if (budget > limit) {
      LOG.warn("{} of {} bytes is more than half of the {} bytes of direct memory available to HNSW"
        + " segments; using {} bytes. Raise -XX:MaxDirectMemorySize to use the configured"
        + " budget.", QueryServices.HNSW_OFFHEAP_MAX_BYTES_ATTRIB, budget, available, limit);
      return limit;
    }
    return budget;
  }

  /** Returns an allocator that publishes no metrics. */
  public HnswOffheapAllocator(long maxBytes) {
    this(maxBytes, null);
  }

  /** Returns an allocator that publishes its metrics to the given source, if it is not null. */
  @VisibleForTesting
  HnswOffheapAllocator(long maxBytes, MetricsVectorIndexSource metrics) {
    this.maxBytes = maxBytes;
    this.metrics = metrics;
    publish(HnswMetric.OFFHEAP_MAX_BYTES, maxBytes);
  }

  private void publish(HnswMetric metric, long value) {
    if (metrics != null) {
      metrics.updateHnsw(metric, value);
    }
  }

  /**
   * Allocates a direct buffer. If the buffer does not fit in the budget, the allocator first evicts
   * the least recently used resident blocks. The caller holds the only reference to the returned
   * block until it calls {@link #commit}.
   * @param size the buffer size in bytes
   * @return a block that the budget counts
   * @throws HnswSegment.TooLargeException if the size is more than the budget
   * @throws IOException                   if blocks that are not resident hold too much of the
   *                                       budget, or if the JVM cannot allocate the direct memory.
   *                                       A retry can succeed.
   */
  Block allocate(int size) throws IOException {
    if (size > maxBytes) {
      publish(HnswMetric.OFFHEAP_REFUSALS, 1);
      throw new HnswSegment.TooLargeException(
        "HNSW segment of " + size + " bytes exceeds the off-heap budget of " + maxBytes + " bytes ("
          + QueryServices.HNSW_OFFHEAP_MAX_BYTES_ATTRIB + ")");
    }
    List<Block> evicted = new ArrayList<>();
    synchronized (this) {
      long evictable = 0;
      for (Block block : resident.values()) {
        evictable += block.buffer.capacity();
      }
      if (allocatedBytes - evictable + size > maxBytes) {
        // Eviction cannot make enough space, so refuse the allocation before any eviction
        publish(HnswMetric.OFFHEAP_REFUSALS, 1);
        throw new IOException("Cannot allocate " + size + " bytes for an HNSW segment: segments"
          + " that are loading or still read after eviction hold " + (allocatedBytes - evictable)
          + " bytes of the off-heap budget of " + maxBytes + " bytes ("
          + QueryServices.HNSW_OFFHEAP_MAX_BYTES_ATTRIB + ")");
      }
      long evicting = 0;
      Iterator<Block> lru = resident.values().iterator();
      while (allocatedBytes - evicting + size > maxBytes && lru.hasNext()) {
        Block victim = lru.next();
        lru.remove();
        evicting += victim.buffer.capacity();
        evicted.add(victim);
      }
      allocatedBytes += size;
      publish(HnswMetric.OFFHEAP_ALLOCATED_BYTES, size);
      if (!evicted.isEmpty()) {
        publish(HnswMetric.OFFHEAP_EVICTIONS, evicted.size());
        publish(HnswMetric.OFFHEAP_EVICTED_BYTES, evicting);
        publish(HnswMetric.OFFHEAP_RESIDENT_SEGMENTS, -evicted.size());
      }
    }
    if (!evicted.isEmpty()) {
      LOG.info("Evicted {} HNSW segments to allocate {} bytes", evicted.size(), size);
    }
    // Run the eviction callbacks outside the lock. Release each victim before the new allocation:
    // the release frees a victim that no search reads, so its memory is available again.
    for (Block victim : evicted) {
      victim.onEvict.run();
      victim.release();
    }
    try {
      return new Block(ByteBuffer.allocateDirect(size));
    } catch (OutOfMemoryError e) {
      discharge(size);
      publish(HnswMetric.OFFHEAP_REFUSALS, 1);
      throw new IOException("Cannot allocate " + size + " bytes of direct memory for an HNSW"
        + " segment; -XX:MaxDirectMemorySize must cover "
        + QueryServices.HNSW_OFFHEAP_MAX_BYTES_ATTRIB, e);
    }
  }

  /**
   * Makes a filled block resident for its owner. The allocator takes its own reference. It drops
   * this reference on eviction, after it calls {@code onEvict}, or on {@link #release}. The caller
   * must still hold its reference.
   * @param loadStart the time at which the load of the block started, for the load time metric
   */
  void commit(Object owner, Block block, Runnable onEvict, long loadStart) {
    block.refs.incrementAndGet();
    Block replaced;
    synchronized (this) {
      block.onEvict = onEvict;
      replaced = resident.put(owner, block);
      if (replaced == null) {
        publish(HnswMetric.OFFHEAP_RESIDENT_SEGMENTS, 1);
      }
      publish(HnswMetric.SEGMENT_LOADS, 1);
      publish(HnswMetric.SEGMENT_LOAD_TIME, EnvironmentEdgeManager.currentTimeMillis() - loadStart);
    }
    if (replaced != null) {
      replaced.release();
    }
  }

  /** Marks the resident block of the owner as the most recently used block. */
  public synchronized void touch(Object owner) {
    resident.get(owner);
  }

  /** Drops the resident block of the owner. The block becomes free when no caller reads it. */
  public void release(Object owner) {
    Block block;
    synchronized (this) {
      block = resident.remove(owner);
      if (block != null) {
        publish(HnswMetric.OFFHEAP_RESIDENT_SEGMENTS, -1);
      }
    }
    if (block != null) {
      block.release();
    }
  }

  /** Evicts all resident segments, as memory pressure does. */
  @VisibleForTesting
  public void evictAll() {
    List<Block> evicted;
    synchronized (this) {
      evicted = new ArrayList<>(resident.values());
      resident.clear();
      long bytes = 0;
      for (Block block : evicted) {
        bytes += block.buffer.capacity();
      }
      publish(HnswMetric.OFFHEAP_EVICTIONS, evicted.size());
      publish(HnswMetric.OFFHEAP_EVICTED_BYTES, bytes);
      publish(HnswMetric.OFFHEAP_RESIDENT_SEGMENTS, -evicted.size());
    }
    for (Block block : evicted) {
      block.onEvict.run();
      block.release();
    }
  }

  private synchronized void discharge(long size) {
    allocatedBytes -= size;
    publish(HnswMetric.OFFHEAP_ALLOCATED_BYTES, -size);
  }

  /** Returns the number of bytes that the budget counts now. */
  @VisibleForTesting
  synchronized long getAllocatedBytes() {
    return allocatedBytes;
  }
}
