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
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.phoenix.memory.MemoryManager.MemoryChunk;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

/**
 *
 * Tests for GlobalMemoryManager and ChildMemoryManager
 * TODO: use our own time keeper so these tests don't flap
 *
 *
 * @since 0.1
 */
public class MemoryManagerTest {
    @Test
    public void testOverGlobalMemoryLimit() throws Exception {
        GlobalMemoryManager gmm = new GlobalMemoryManager(250,1);
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


    private static void sleepFor(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException x) {
            fail();
        }
    }

    @Ignore("See PHOENIX-2840")
    @Test
    public void testWaitForMemoryAvailable() throws Exception {
        final GlobalMemoryManager gmm = spy(new GlobalMemoryManager(100, 80));
        final ChildMemoryManager rmm1 = new ChildMemoryManager(gmm,100);
        final ChildMemoryManager rmm2 = new ChildMemoryManager(gmm,100);
        final CountDownLatch latch = new CountDownLatch(2);
        Thread t1 = new Thread() {
            @Override
            public void run() {
                MemoryChunk c1 = rmm1.allocate(50);
                MemoryChunk c2 = rmm1.allocate(50);
                sleepFor(40);
                c1.close();
                sleepFor(20);
                c2.close();
                latch.countDown();
            }
        };
        Thread t2 = new Thread() {
            @Override
            public void run() {
                sleepFor(20);
                // Will require waiting for a bit of time before t1 frees the requested memory
                MemoryChunk c3 = rmm2.allocate(50);
                Mockito.verify(gmm, atLeastOnce()).waitForBytesToFree(anyLong(), anyLong());
                c3.close();
                latch.countDown();
            }
        };
        t2.start();
        t1.start();
        latch.await(1, TimeUnit.SECONDS);
        // Main thread competes with others to get all memory, but should wait
        // until both threads are complete (since that's when the memory will
        // again be all available.
        ChildMemoryManager rmm = new ChildMemoryManager(gmm,100);
        MemoryChunk c = rmm.allocate(100);
        c.close();
        assertTrue(rmm.getAvailableMemory() == rmm.getMaxMemory());
        assertTrue(rmm1.getAvailableMemory() == rmm1.getMaxMemory());
        assertTrue(rmm2.getAvailableMemory() == rmm2.getMaxMemory());
    }

    @Ignore("See PHOENIX-2840")
    @Test
    public void testResizeWaitForMemoryAvailable() throws Exception {
        final GlobalMemoryManager gmm = spy(new GlobalMemoryManager(100, 80));
        final ChildMemoryManager rmm1 = new ChildMemoryManager(gmm,100);
        final ChildMemoryManager rmm2 = new ChildMemoryManager(gmm,100);
        final CountDownLatch latch = new CountDownLatch(2);
        Thread t1 = new Thread() {
            @Override
            public void run() {
                MemoryChunk c1 = rmm1.allocate(50);
                MemoryChunk c2 = rmm1.allocate(40);
                sleepFor(40);
                c1.close();
                sleepFor(20);
                c2.close();
                latch.countDown();
            }
        };
        Thread t2 = new Thread() {
            @Override
            public void run() {
                sleepFor(20);
                MemoryChunk c3 = rmm2.allocate(10);
                // Will require waiting for a bit of time before t1 frees the requested memory
                c3.resize(50);
                Mockito.verify(gmm, atLeastOnce()).waitForBytesToFree(anyLong(), anyLong());
                c3.close();
                latch.countDown();
            }
        };
        t1.start();
        t2.start();
        latch.await(1, TimeUnit.SECONDS);
        // Main thread competes with others to get all memory, but should wait
        // until both threads are complete (since that's when the memory will
        // again be all available.
        ChildMemoryManager rmm = new ChildMemoryManager(gmm,100);
        MemoryChunk c = rmm.allocate(100);
        c.close();
        assertTrue(rmm.getAvailableMemory() == rmm.getMaxMemory());
        assertTrue(rmm1.getAvailableMemory() == rmm1.getMaxMemory());
        assertTrue(rmm2.getAvailableMemory() == rmm2.getMaxMemory());
    }

    @Ignore("See PHOENIX-2840")
    @Test
    public void testWaitUntilResize() throws Exception {
        final GlobalMemoryManager gmm = spy(new GlobalMemoryManager(100, 80));
        final ChildMemoryManager rmm1 = new ChildMemoryManager(gmm,100);
        final MemoryChunk c1 = rmm1.allocate(70);
        final CountDownLatch latch = new CountDownLatch(2);

        Thread t1 = new Thread() {
            @Override
            public void run() {
                MemoryChunk c2 = rmm1.allocate(20);
                sleepFor(40);
                c1.resize(20); // resize down to test that other thread is notified
                sleepFor(20);
                c2.close();
                c1.close();
                assertTrue(rmm1.getAvailableMemory() == rmm1.getMaxMemory());
                latch.countDown();
            }
        };
        Thread t2 = new Thread() {
            @Override
            public void run() {
                sleepFor(20);
                ChildMemoryManager rmm2 = new ChildMemoryManager(gmm,100);
                MemoryChunk c3 = rmm2.allocate(10);
                long startTime = System.currentTimeMillis();
                c3.resize(60); // Test that resize waits if memory not available
                assertTrue(c1.getSize() == 20); // c1 was resized not closed
                // we waited some time before the allocate happened

                Mockito.verify(gmm, atLeastOnce()).waitForBytesToFree(anyLong(), anyLong());
                c3.close();
                assertTrue(rmm2.getAvailableMemory() == rmm2.getMaxMemory());
                latch.countDown();
            }
        };
        t1.start();
        t2.start();
        latch.await(1, TimeUnit.SECONDS);
        // Main thread competes with others to get all memory, but should wait
        // until both threads are complete (since that's when the memory will
        // again be all available.
        ChildMemoryManager rmm = new ChildMemoryManager(gmm,100);
        MemoryChunk c = rmm.allocate(100);
        c.close();
        assertTrue(rmm.getAvailableMemory() == rmm.getMaxMemory());
    }

    @Test
    public void testChildDecreaseAllocation() throws Exception {
        MemoryManager gmm = spy(new GlobalMemoryManager(100, 1));
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
        MemoryManager gmm = new GlobalMemoryManager(100,1);
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
}
