/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.memory;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.phoenix.SystemExitRule;
import org.apache.phoenix.coprocessor.GroupedAggregateRegionObserver;
import org.apache.phoenix.memory.MemoryManager.MemoryChunk;
import org.junit.ClassRule;
import org.junit.Test;

/**
 *
 * Tests for GlobalMemoryManager and ChildMemoryManager
 * TODO: use our own time keeper so these tests don't flap
 *
 *
 * @since 0.1
 */
public class MemoryManagerTest {

    @ClassRule
    public static final SystemExitRule SYSTEM_EXIT_RULE = new SystemExitRule();

    @Test
    public void testOverGlobalMemoryLimit() throws Exception {
        GlobalMemoryManager gmm = new GlobalMemoryManager(250);
        try {
            gmm.allocate(300);
            fail();
        } catch (InsufficientMemoryException e) { // expected
        }

        ChildMemoryManager rmm1 = new ChildMemoryManager(gmm,100);
        ChildMemoryManager rmm2 = new ChildMemoryManager(gmm,100);
        MemoryChunk c1 = rmm1.allocate(100);
        MemoryChunk c2 = rmm2.allocate(100);
        try {
            rmm2.allocate(100);
            fail();
        } catch (InsufficientMemoryException e) { // expected
        }

        c1.close();
        c2.close();
        assertTrue(rmm1.getAvailableMemory() == rmm1.getMaxMemory());
    }

    @Test
    public void testChildDecreaseAllocation() throws Exception {
        MemoryManager gmm = spy(new GlobalMemoryManager(100));
        ChildMemoryManager rmm1 = new ChildMemoryManager(gmm,100);
        ChildMemoryManager rmm2 = new ChildMemoryManager(gmm,10);
        MemoryChunk c1 = rmm1.allocate(50);
        MemoryChunk c2 = rmm2.allocate(5,50);
        assertTrue(c2.getSize() == 10);
        c1.close();
        assertTrue(rmm1.getAvailableMemory() == rmm1.getMaxMemory());
        c2.close();
        assertTrue(rmm2.getAvailableMemory() == rmm2.getMaxMemory());
        assertTrue(gmm.getAvailableMemory() == gmm.getMaxMemory());
    }

    @Test
    public void testOverChildMemoryLimit() throws Exception {
        MemoryManager gmm = new GlobalMemoryManager(100);
        ChildMemoryManager rmm1 = new ChildMemoryManager(gmm,25);
        ChildMemoryManager rmm2 = new ChildMemoryManager(gmm,25);
        ChildMemoryManager rmm3 = new ChildMemoryManager(gmm,25);
        ChildMemoryManager rmm4 = new ChildMemoryManager(gmm,35);
        MemoryChunk c1 = rmm1.allocate(20);
        MemoryChunk c2 = rmm2.allocate(20);
        try {
            rmm1.allocate(10);
            fail();
        } catch (InsufficientMemoryException e) { // expected
        }
        MemoryChunk c3 = rmm3.allocate(25);
        c1.close();
        // Ensure that you can get back to max for rmn1 after failure
        MemoryChunk c4 = rmm1.allocate(10);
        MemoryChunk c5 = rmm1.allocate(15);

        MemoryChunk c6 = rmm4.allocate(25);
        try {
            // This passes % test, but fails the next total memory usage test
            rmm4.allocate(10);
            fail();
        } catch (InsufficientMemoryException e) { // expected
        }
        c2.close();
        // Tests that % test passes (confirming that the 10 above was subtracted back from request memory usage,
        // since we'd be at the max of 35% now
        MemoryChunk c7 = rmm4.allocate(10);

        try {
            rmm4.allocate(1);
            fail();
        } catch (InsufficientMemoryException e) { // expected
        }

        try {
            rmm2.allocate(25);
            fail();
        } catch (InsufficientMemoryException e) { // expected
        }

        c3.close();
        c4.close();
        c5.close();
        c6.close();
        c7.close();
        assertTrue(rmm1.getAvailableMemory() == rmm1.getMaxMemory());
        assertTrue(rmm2.getAvailableMemory() == rmm2.getMaxMemory());
        assertTrue(rmm3.getAvailableMemory() == rmm3.getMaxMemory());
        assertTrue(rmm4.getAvailableMemory() == rmm4.getMaxMemory());
    }

    @Test
    public void testConcurrentAllocation() throws Exception {
        int THREADS = 100;

        // each thread will attempt up to 100 allocations on average.
        final GlobalMemoryManager gmm = new GlobalMemoryManager(THREADS * 1000);
        final AtomicInteger count = new AtomicInteger(0);
        final CountDownLatch barrier = new CountDownLatch(THREADS);
        final CountDownLatch barrier2 = new CountDownLatch(THREADS);
        final CountDownLatch signal = new CountDownLatch(1);
        /*
         * each thread will allocate chunks of 10 bytes, until no more memory is available.
         */
        for (int i = 0; i < THREADS; i++) {
            new Thread(new Runnable() {
                List<MemoryChunk> chunks = new ArrayList<>();
                @Override
                public void run() {
                    try {
                        while(true) {
                            Thread.sleep(1);
                            chunks.add(gmm.allocate(10));
                            count.incrementAndGet();
                        }
                    } catch (InsufficientMemoryException e) {
                        barrier.countDown();
                        // wait for the signal to go ahead
                        try {signal.await();} catch (InterruptedException ix) {}
                        for (MemoryChunk chunk : chunks) {
                            chunk.close();
                        }
                        barrier2.countDown();
                    } catch (InterruptedException ix) {}
                }
            }).start();
        }
        // wait until all threads failed an allocation
        barrier.await();
        // make sure all memory was used
        assertTrue(gmm.getAvailableMemory() == 0);
        // let the threads end, and free their memory
        signal.countDown(); barrier2.await();
        // make sure all memory is freed
        assertTrue(gmm.getAvailableMemory() == gmm.getMaxMemory());
    }

    /**
     * Test for SpillableGroupByCache which is using MemoryManager to allocate chunks for GroupBy execution
     * @throws Exception
     */
    @Test
    public void testCorrectnessOfChunkAllocation() throws Exception {
        for(int i = 1000;i < Integer.MAX_VALUE;) {
            i *=1.5f;
            long result = GroupedAggregateRegionObserver.sizeOfUnorderedGroupByMap(i, 100);
            assertTrue("Size for GroupByMap is negative" , result > 0);
        }
    }

}
