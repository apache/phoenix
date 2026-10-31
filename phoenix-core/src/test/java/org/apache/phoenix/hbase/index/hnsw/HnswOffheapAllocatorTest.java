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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.regionserver.MemStoreLAB;
import org.apache.phoenix.hbase.index.metrics.HnswMetricValues;
import org.apache.phoenix.hbase.index.metrics.MetricsVectorIndexSource.HnswMetric;
import org.apache.phoenix.hbase.index.metrics.MetricsVectorIndexSourceImpl;
import org.apache.phoenix.query.QueryServices;
import org.junit.Test;

public class HnswOffheapAllocatorTest {
  private static final AtomicInteger SOURCES = new AtomicInteger();

  // Allocates a block and makes it resident for the owner, as a segment load does
  private static HnswOffheapAllocator.Block resident(HnswOffheapAllocator allocator, String owner,
    int size, List<String> evicted) throws IOException {
    HnswOffheapAllocator.Block block = allocator.allocate(size);
    allocator.commit(owner, block, () -> evicted.add(owner), 0);
    block.release();
    return block;
  }

  // Returns a new metrics source with a unique name, so that a test reads only its own values
  private static MetricsVectorIndexSourceImpl newSource() {
    String name = "HnswAllocatorTest" + SOURCES.incrementAndGet();
    return new MetricsVectorIndexSourceImpl(name, name, "phoenix", "RegionServer,sub=" + name);
  }

  private static void assertMetric(MetricsVectorIndexSourceImpl source, HnswMetric metric,
    long expected) {
    assertEquals(metric.metricName(), expected, HnswMetricValues.value(source, metric, null));
  }

  /**
   * The allocator of a server publishes its budget, its allocated bytes and its resident segments
   * as gauges. It counts loads, evictions and refused allocations. The gauges return to 0 when no
   * block is allocated.
   */
  @Test
  public void testMetrics() throws Exception {
    MetricsVectorIndexSourceImpl source = newSource();
    HnswOffheapAllocator allocator = new HnswOffheapAllocator(100, source);
    List<String> evicted = new ArrayList<>();
    assertMetric(source, HnswMetric.OFFHEAP_MAX_BYTES, 100);
    resident(allocator, "a", 40, evicted);
    resident(allocator, "b", 30, evicted);
    assertMetric(source, HnswMetric.OFFHEAP_ALLOCATED_BYTES, 70);
    assertMetric(source, HnswMetric.OFFHEAP_RESIDENT_SEGMENTS, 2);
    assertMetric(source, HnswMetric.SEGMENT_LOADS, 2);
    assertMetric(source, HnswMetric.SEGMENT_LOAD_TIME, 2);
    // A new owner block replaces the old one, which is freed without eviction
    resident(allocator, "b", 20, evicted);
    assertMetric(source, HnswMetric.OFFHEAP_RESIDENT_SEGMENTS, 2);
    assertMetric(source, HnswMetric.OFFHEAP_ALLOCATED_BYTES, 60);
    assertMetric(source, HnswMetric.OFFHEAP_EVICTIONS, 0);

    // The load evicts the least recently used block
    resident(allocator, "c", 50, evicted);
    assertEquals(Collections.singletonList("a"), evicted);
    assertMetric(source, HnswMetric.OFFHEAP_EVICTIONS, 1);
    assertMetric(source, HnswMetric.OFFHEAP_EVICTED_BYTES, 40);
    assertMetric(source, HnswMetric.OFFHEAP_RESIDENT_SEGMENTS, 2);
    assertMetric(source, HnswMetric.OFFHEAP_ALLOCATED_BYTES, 70);
    assertMetric(source, HnswMetric.SEGMENT_LOADS, 4);

    // Memory pressure evicts the other blocks, and an evicted segment loads again
    allocator.evictAll();
    assertMetric(source, HnswMetric.OFFHEAP_EVICTIONS, 3);
    assertMetric(source, HnswMetric.OFFHEAP_EVICTED_BYTES, 110);
    assertMetric(source, HnswMetric.OFFHEAP_RESIDENT_SEGMENTS, 0);
    assertMetric(source, HnswMetric.OFFHEAP_ALLOCATED_BYTES, 0);
    resident(allocator, "c", 40, evicted);
    assertMetric(source, HnswMetric.SEGMENT_LOADS, 5);
    assertMetric(source, HnswMetric.OFFHEAP_RESIDENT_SEGMENTS, 1);

    // Refusals: a block that loads cannot be evicted, and a block can be larger than the budget
    HnswOffheapAllocator.Block loading = allocator.allocate(60);
    assertRefused(allocator, 50);
    try {
      allocator.allocate(101);
      fail("expected TooLargeException");
    } catch (HnswSegment.TooLargeException expected) {
    }
    assertMetric(source, HnswMetric.OFFHEAP_REFUSALS, 2);
    assertMetric(source, HnswMetric.OFFHEAP_ALLOCATED_BYTES, 100);
    loading.release();
    allocator.release("c");
    assertMetric(source, HnswMetric.OFFHEAP_ALLOCATED_BYTES, 0);
    assertMetric(source, HnswMetric.OFFHEAP_RESIDENT_SEGMENTS, 0);
    assertMetric(source, HnswMetric.OFFHEAP_MAX_BYTES, 100);
  }

  /**
   * An allocator of a JVM without a RegionServer, such as the allocator of an fsck tool, publishes
   * nothing. The segments of a tool must not count as segments of a server in the same JVM.
   */
  @Test
  public void testClientAllocatorPublishesNothing() throws Exception {
    List<HnswMetric> serverOnly = new ArrayList<>();
    List<Long> before = new ArrayList<>();
    for (HnswMetric metric : HnswMetric.values()) {
      if (metric.serverOnly()) {
        serverOnly.add(metric);
        before.add(HnswMetricValues.value(metric));
      }
    }
    for (HnswOffheapAllocator allocator : new HnswOffheapAllocator[] {
      HnswOffheapAllocator.getForClient(new Configuration()), new HnswOffheapAllocator(100) }) {
      List<String> evicted = new ArrayList<>();
      resident(allocator, "client-a", 60, evicted);
      resident(allocator, "client-b", 60, evicted);
      allocator.evictAll();
    }
    for (int i = 0; i < serverOnly.size(); i++) {
      assertEquals(serverOnly.get(i).metricName(), (long) before.get(i),
        HnswMetricValues.value(serverOnly.get(i)));
    }
  }

  @Test
  public void testEvictsLeastRecentlyUsed() throws Exception {
    HnswOffheapAllocator allocator = new HnswOffheapAllocator(300);
    List<String> evicted = new ArrayList<>();
    resident(allocator, "a", 100, evicted);
    resident(allocator, "b", 100, evicted);
    resident(allocator, "c", 100, evicted);
    allocator.touch("a");
    // 150 bytes do not fit, so the allocation evicts the least recently used blocks, b and c
    resident(allocator, "d", 150, evicted);
    assertEquals(Arrays.asList("b", "c"), evicted);
    assertEquals(250, allocator.getAllocatedBytes());

    allocator.release("a");
    assertEquals(150, allocator.getAllocatedBytes());
    resident(allocator, "e", 150, evicted);
    assertEquals("Explicitly released allocations should not trigger eviction callbacks",
      Arrays.asList("b", "c"), evicted);
  }

  @Test
  public void testCommitReplacesOwnersBlock() throws Exception {
    HnswOffheapAllocator allocator = new HnswOffheapAllocator(200);
    List<String> evicted = new ArrayList<>();
    resident(allocator, "a", 80, evicted);
    resident(allocator, "a", 90, evicted);
    assertEquals("A replaced block is freed without its eviction callback", Collections.emptyList(),
      evicted);
    assertEquals(90, allocator.getAllocatedBytes());
  }

  @Test
  public void testAllocationLargerThanBudgetFails() throws Exception {
    HnswOffheapAllocator allocator = new HnswOffheapAllocator(100);
    List<String> evicted = new ArrayList<>();
    resident(allocator, "a", 60, evicted);
    try {
      allocator.allocate(101);
      fail("expected TooLargeException");
    } catch (HnswSegment.TooLargeException expected) {
    }
    assertEquals("Failed allocation request should not evict existing entries",
      Collections.emptyList(), evicted);
    assertEquals(60, allocator.getAllocatedBytes());
  }

  @Test(timeout = 30000)
  public void testEvictionCallbackRunsOutsideLock() throws Exception {
    HnswOffheapAllocator allocator = new HnswOffheapAllocator(100);
    ExecutorService other = Executors.newSingleThreadExecutor();
    try {
      CountDownLatch called = new CountDownLatch(1);
      HnswOffheapAllocator.Block a = allocator.allocate(100);
      allocator.commit("a", a, () -> {
        Future<?> f = other.submit(() -> allocator.release("unrelated"));
        try {
          f.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
          throw new AssertionError("allocator lock held during eviction callback", e);
        }
        called.countDown();
      }, 0);
      a.release();
      allocator.allocate(100).release();
      assertTrue(called.await(0, TimeUnit.SECONDS));
    } finally {
      other.shutdownNow();
    }
  }

  /**
   * A block that a segment load has not committed is not resident, so an allocation cannot evict
   * it. The budget counts the block until the load commits or releases it. The allocator refuses a
   * concurrent load that does not fit next to it.
   */
  @Test
  public void testLoadingBlockIsNotEvicted() throws Exception {
    HnswOffheapAllocator allocator = new HnswOffheapAllocator(100);
    List<String> evicted = new ArrayList<>();
    HnswOffheapAllocator.Block loading = allocator.allocate(60);
    assertRefused(allocator, 60);
    assertEquals(60, allocator.getAllocatedBytes());
    HnswOffheapAllocator.Block other = allocator.allocate(40);
    assertEquals(100, allocator.getAllocatedBytes());
    allocator.commit("loading", loading, () -> evicted.add("loading"), 0);
    loading.release();
    other.release();
    assertEquals(60, allocator.getAllocatedBytes());
    // After the commit, the block is resident, and memory pressure can evict it
    resident(allocator, "next", 60, evicted);
    assertEquals(Collections.singletonList("loading"), evicted);
    assertEquals(60, allocator.getAllocatedBytes());
  }

  /**
   * The budget counts evicted blocks that searches still read. If eviction cannot make space for an
   * allocation next to these blocks, the allocator refuses it and evicts nothing. The allocation
   * succeeds after the searches end.
   */
  @Test
  public void testAllocationThatEvictionCannotFitIsRefused() throws Exception {
    HnswOffheapAllocator allocator = new HnswOffheapAllocator(100);
    List<String> evicted = new ArrayList<>();
    HnswOffheapAllocator.Block searched = resident(allocator, "a", 60, evicted);
    assertTrue(searched.retain());
    // Eviction of the searched block makes space. Until the search ends, the counted bytes are
    // more than the budget.
    resident(allocator, "b", 60, evicted);
    assertEquals(Collections.singletonList("a"), evicted);
    assertEquals(120, allocator.getAllocatedBytes());
    assertRefused(allocator, 50);
    assertEquals("A refused allocation evicts nothing", Collections.singletonList("a"), evicted);
    assertEquals(120, allocator.getAllocatedBytes());
    searched.release();
    assertEquals(60, allocator.getAllocatedBytes());
    resident(allocator, "c", 50, evicted);
    assertEquals(Arrays.asList("a", "b"), evicted);
    assertEquals(50, allocator.getAllocatedBytes());
  }

  private static void assertRefused(HnswOffheapAllocator allocator, int size) {
    try {
      allocator.allocate(size).release();
      fail("an allocation that the budget cannot fit must be refused");
    } catch (IOException e) {
      assertFalse("a retry can succeed", e instanceof HnswSegment.TooLargeException);
    }
  }

  /**
   * The budget counts an evicted block that a search still reads. The release of the search frees
   * the direct buffer immediately.
   */
  @Test
  public void testEvictedBlockIsFreedByLastRelease() throws Exception {
    BufferPoolMXBean direct = null;
    for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
      if ("direct".equals(pool.getName())) {
        direct = pool;
      }
    }
    int size = 64 << 20;
    HnswOffheapAllocator allocator = new HnswOffheapAllocator(size);
    List<String> evicted = new ArrayList<>();
    long before = direct.getMemoryUsed();
    HnswOffheapAllocator.Block block = resident(allocator, "a", size, evicted);
    assertTrue(block.retain());
    allocator.evictAll();
    assertEquals(Collections.singletonList("a"), evicted);
    assertEquals(size, allocator.getAllocatedBytes());
    assertTrue(direct.getMemoryUsed() > before + size / 2);
    block.release();
    assertEquals(0, allocator.getAllocatedBytes());
    assertFalse("a freed block cannot be retained", block.retain());
    assertTrue("the last release frees the direct buffer",
      direct.getMemoryUsed() < before + size / 2);
  }

  /**
   * The budget is at most half of the direct memory. In a RegionServer, an off-heap BucketCache and
   * an off-heap memstore use part of the direct memory. Another JVM, such as an fsck tool, can read
   * the same configuration, but it has neither of them.
   */
  @Test
  public void testBudgetLimitedByDirectMemory() {
    long gb = 1L << 30;
    Configuration conf = new Configuration(false);
    assertEquals(2 * gb, HnswOffheapAllocator.budget(conf, 8 * gb, true));
    assertEquals(gb, HnswOffheapAllocator.budget(conf, 2 * gb, true));
    conf.set(HConstants.BUCKET_CACHE_IOENGINE_KEY, "file:/bucketcache");
    conf.setFloat(HConstants.BUCKET_CACHE_SIZE_KEY, 40 * 1024);
    assertEquals("a file BucketCache uses no direct memory", 2 * gb,
      HnswOffheapAllocator.budget(conf, 8 * gb, true));
    conf.set(HConstants.BUCKET_CACHE_IOENGINE_KEY, "offheap");
    assertEquals(gb, HnswOffheapAllocator.budget(conf, 42 * gb, true));
    assertEquals(0, HnswOffheapAllocator.budget(conf, 40 * gb, true));
    assertEquals("an fsck tool hosts no BucketCache", gb,
      HnswOffheapAllocator.budget(conf, 2 * gb, false));

    // The off-heap memstore size is in megabytes. An off-heap memstore needs MSLAB.
    conf.setLong(MemorySizeUtil.OFFHEAP_MEMSTORE_SIZE_KEY, 8 * 1024);
    assertEquals(gb, HnswOffheapAllocator.budget(conf, 50 * gb, true));
    assertEquals("an fsck tool hosts no memstore", gb,
      HnswOffheapAllocator.budget(conf, 2 * gb, false));
    conf.setBoolean(MemStoreLAB.USEMSLAB_KEY, false);
    assertEquals("without MSLAB the memstore is on the heap", 2 * gb,
      HnswOffheapAllocator.budget(conf, 50 * gb, true));

    conf.setLong(QueryServices.HNSW_OFFHEAP_MAX_BYTES_ATTRIB, gb / 2);
    assertEquals(gb / 2, HnswOffheapAllocator.budget(conf, 42 * gb, true));
    assertEquals(gb / 2, HnswOffheapAllocator.budget(conf, 2 * gb, false));
  }
}
