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
package org.apache.phoenix.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.phoenix.jdbc.HighAvailabilityGroup.HAGroupInfo;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

public class ParallelPhoenixContextTest {

    List<PhoenixHAExecutorServiceProvider.PhoenixHAClusterExecutorServices> executorList;

    @Before
    public void init() {
        executorList =
                Lists.newArrayList(new PhoenixHAExecutorServiceProvider.PhoenixHAClusterExecutorServices(new TrackingThreadPoolExecutor(),new TrackingThreadPoolExecutor()),
                        new PhoenixHAExecutorServiceProvider.PhoenixHAClusterExecutorServices(new TrackingThreadPoolExecutor(),new TrackingThreadPoolExecutor()));
    }

    private static class TrackingThreadPoolExecutor extends ThreadPoolExecutor {

        AtomicInteger tasksExecuted = new AtomicInteger();

        public TrackingThreadPoolExecutor() {
            super(1, 1, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        }

        @Override
        public void execute(Runnable r) {
            super.execute(r);
            tasksExecuted.incrementAndGet();
        }
    }

    @Test
    public void testContructionFailsWithLessThan2ThreadPools() {
        try {
            ParallelPhoenixContext context =
                    new ParallelPhoenixContext(new Properties(),
                            Mockito.mock(HighAvailabilityGroup.class),
                            Lists.newArrayList(Mockito.mock(PhoenixHAExecutorServiceProvider.PhoenixHAClusterExecutorServices.class)), null);
            fail("Should not construct with less than 2 ThreadPools");
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testPool1OutOfCapacity() throws Exception {
        HAGroupInfo haGroupInfo = new HAGroupInfo("test", "test1", "test2");
        ParallelPhoenixContext context =
                new ParallelPhoenixContext(new Properties(),
                        new HighAvailabilityGroup(haGroupInfo,
                                Mockito.mock(Properties.class),
                                Mockito.mock(ClusterRoleRecord.class),
                                HighAvailabilityGroup.State.READY),
                        executorList, Lists.newArrayList(Boolean.FALSE, Boolean.TRUE));
        CompletableFuture<Boolean> future1 = context.chainOnConn1(() -> true);
        assertTrue(future1.isCompletedExceptionally());
        assertEquals(0, ((TrackingThreadPoolExecutor) executorList.get(0).getExecutorService()).tasksExecuted.get());
        assertEquals(0, ((TrackingThreadPoolExecutor) executorList.get(1).getExecutorService()).tasksExecuted.get());
        CompletableFuture<Boolean> future2 = context.chainOnConn2(() -> true);
        assertTrue(future2.get());
        assertEquals(0, ((TrackingThreadPoolExecutor) executorList.get(0).getExecutorService()).tasksExecuted.get());
        assertEquals(1, ((TrackingThreadPoolExecutor) executorList.get(1).getExecutorService()).tasksExecuted.get());
    }

    @Test
    public void testPool2OutOfCapacity() throws Exception {
        HAGroupInfo haGroupInfo = new HAGroupInfo("test", "test1", "test2");
        ParallelPhoenixContext context =
                new ParallelPhoenixContext(new Properties(),
                        new HighAvailabilityGroup(haGroupInfo,
                                Mockito.mock(Properties.class),
                                Mockito.mock(ClusterRoleRecord.class),
                                HighAvailabilityGroup.State.READY),
                        executorList, Lists.newArrayList(Boolean.TRUE, Boolean.FALSE));
        CompletableFuture<Boolean> future1 = context.chainOnConn1(() -> true);
        assertTrue(future1.get());
        assertEquals(1, ((TrackingThreadPoolExecutor) executorList.get(0).getExecutorService()).tasksExecuted.get());
        assertEquals(0, ((TrackingThreadPoolExecutor) executorList.get(1).getExecutorService()).tasksExecuted.get());
        CompletableFuture<Boolean> future2 = context.chainOnConn2(() -> true);
        assertTrue(future2.isCompletedExceptionally());
        assertEquals(1, ((TrackingThreadPoolExecutor) executorList.get(0).getExecutorService()).tasksExecuted.get());
        assertEquals(0, ((TrackingThreadPoolExecutor) executorList.get(1).getExecutorService()).tasksExecuted.get());
    }

    @Test
    public void testPoolsHaveCapacity() throws Exception {
        ParallelPhoenixContext context =
                new ParallelPhoenixContext(new Properties(),
                        Mockito.mock(HighAvailabilityGroup.class), executorList,
                        Lists.newArrayList(Boolean.TRUE, Boolean.TRUE));
        CompletableFuture<Boolean> future1 = context.chainOnConn1(() -> true);
        assertTrue(future1.get());
        assertEquals(1, ((TrackingThreadPoolExecutor) executorList.get(0).getExecutorService()).tasksExecuted.get());
        assertEquals(0, ((TrackingThreadPoolExecutor) executorList.get(1).getExecutorService()).tasksExecuted.get());
        CompletableFuture<Boolean> future2 = context.chainOnConn2(() -> true);
        assertTrue(future2.get());
        assertEquals(1, ((TrackingThreadPoolExecutor) executorList.get(0).getExecutorService()).tasksExecuted.get());
        assertEquals(1, ((TrackingThreadPoolExecutor) executorList.get(1).getExecutorService()).tasksExecuted.get());
    }
}
