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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class HnswOffheapAllocatorTest {

  @Test
  public void testEvictsLeastRecentlyUsed() throws Exception {
    HnswOffheapAllocator allocator = new HnswOffheapAllocator(300);
    List<String> evicted = new ArrayList<>();
    allocator.allocate("a", 100, () -> evicted.add("a"));
    allocator.allocate("b", 100, () -> evicted.add("b"));
    allocator.allocate("c", 100, () -> evicted.add("c"));
    allocator.touch("a");
    // Allocating 150 bytes requires evicting the least recently used entries ('b' and 'c')
    allocator.allocate("d", 150, () -> evicted.add("d"));
    assertEquals(Arrays.asList("b", "c"), evicted);
    assertEquals(250, allocator.getAllocatedBytes());

    allocator.release("a");
    assertEquals(150, allocator.getAllocatedBytes());
    allocator.allocate("e", 150, () -> evicted.add("e"));
    assertEquals("Explicitly released allocations should not trigger eviction callbacks",
      Arrays.asList("b", "c"), evicted);
  }

  @Test
  public void testReallocationReplacesOwnersBytes() throws Exception {
    HnswOffheapAllocator allocator = new HnswOffheapAllocator(100);
    allocator.allocate("a", 80,
      () -> fail("Reallocating for the same owner should not trigger eviction"));
    allocator.allocate("a", 90, () -> {
    });
    assertEquals(90, allocator.getAllocatedBytes());
  }

  @Test
  public void testAllocationLargerThanBudgetFails() throws Exception {
    HnswOffheapAllocator allocator = new HnswOffheapAllocator(100);
    allocator.allocate("a", 60,
      () -> fail("Failed allocation request should not evict existing entries"));
    try {
      allocator.allocate("b", 101, () -> {
      });
      fail("expected IOException");
    } catch (IOException expected) {
    }
    assertEquals(60, allocator.getAllocatedBytes());
  }

  @Test(timeout = 30000)
  public void testEvictionCallbackRunsOutsideLock() throws Exception {
    HnswOffheapAllocator allocator = new HnswOffheapAllocator(100);
    ExecutorService other = Executors.newSingleThreadExecutor();
    try {
      CountDownLatch called = new CountDownLatch(1);
      allocator.allocate("a", 100, () -> {
        Future<?> f = other.submit(() -> allocator.release("unrelated"));
        try {
          f.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
          throw new AssertionError("allocator lock held during eviction callback", e);
        }
        called.countDown();
      });
      allocator.allocate("b", 100, () -> {
      });
      assertTrue(called.await(0, TimeUnit.SECONDS));
    } finally {
      other.shutdownNow();
    }
  }
}
