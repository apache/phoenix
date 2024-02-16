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

import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

public class ParallelPhoenixUtilTest {

    ParallelPhoenixUtil util = ParallelPhoenixUtil.INSTANCE;

    private static final ParallelPhoenixContext context =
            new ParallelPhoenixContext(new Properties(), null,
                    HighAvailabilityTestingUtility.getListOfSingleThreadExecutorServices(), null);

    @Test
    public void getAnyOfNonExceptionallySingleFutureTest() throws Exception {
        String value = "done";
        CompletableFuture<String> future = CompletableFuture.completedFuture(value);

        List<CompletableFuture<?>> futures = new ArrayList<>();
        futures.add(future);
        String result = (String) util.getAnyOfNonExceptionally(futures, context);
        assertEquals(value,result);
    }

    @Test
    public void getAnyOfNonExceptionallyAllFailedFutureTest() throws Exception {
        CompletableFuture<String> future = new CompletableFuture<>();
        future.completeExceptionally(new RuntimeException("Err"));

        List<CompletableFuture<?>> futures = new ArrayList<>();
        futures.add(future);
        try {
            util.getAnyOfNonExceptionally(futures, context);
            fail();
        } catch (SQLException e) {
        }
    }

    @Test
    public void getAnyOfNonExceptionallyMultipleFuturesTest() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Executor delayedExecutor = getLatchedMockExecutor(latch);

        CompletableFuture<String> future1 = CompletableFuture.supplyAsync(() -> "1", delayedExecutor);
        CompletableFuture<String> future2 = CompletableFuture.supplyAsync(() -> "2", delayedExecutor);
        CompletableFuture<String> future3 = CompletableFuture.supplyAsync(() -> "3"); //No delay
        CompletableFuture<String> future4 = CompletableFuture.supplyAsync(() -> "4", delayedExecutor);

        List<CompletableFuture<?>> futures = new ArrayList<>();
        futures.add(future1);
        futures.add(future2);
        futures.add(future3);
        futures.add(future4);
        String result = (String) util.getAnyOfNonExceptionally(futures, context);
        assertEquals("3",result);
    }

    @Test
    public void getAnyOfNonExceptionallyTimeoutTest() throws Exception {
        CompletableFuture<String> future1 = new CompletableFuture<>();
        future1.completeExceptionally(new RuntimeException("Err"));

        CompletableFuture<String> future2 = CompletableFuture.supplyAsync(() -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new CompletionException(e);
            }
            return "Success";
        });
        List<CompletableFuture<?>> futures = new ArrayList<>();
        futures.add(future1);
        futures.add(future2);

        Properties props = new Properties();
        props.setProperty(ParallelPhoenixUtil.PHOENIX_HA_PARALLEL_OPERATION_TIMEOUT_ATTRIB, "2000");
        ParallelPhoenixContext ctx =
                new ParallelPhoenixContext(props, null,
                        HighAvailabilityTestingUtility.getListOfSingleThreadExecutorServices(),
                        null);
        long startTime = EnvironmentEdgeManager.currentTime();
        try {
            util.getAnyOfNonExceptionally(futures, ctx);
            fail("Should've timedout");
        } catch (SQLException e) {
            long elapsedTime = EnvironmentEdgeManager.currentTime() - startTime;
            assertTrue(elapsedTime >= 2000);
            assertEquals(SQLExceptionCode.OPERATION_TIMED_OUT.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void getAnyOfNonExceptionallyFailedFuturesFinishFirstTest() throws Exception {
        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        Executor executor1 = getLatchedMockExecutor(latch1);
        Executor executor2 = getLatchedMockExecutor(latch2);

        CompletableFuture<String> future1 = CompletableFuture.supplyAsync(() -> {throw new RuntimeException();}, executor1);
        CompletableFuture<String> future2 = CompletableFuture.supplyAsync(() -> {throw new RuntimeException();}, executor1);
        CompletableFuture<String> future3 = CompletableFuture.supplyAsync(() -> "3",executor2);
        CompletableFuture<String> future4 = CompletableFuture.supplyAsync(() -> {throw new RuntimeException();}, executor1);

        List<CompletableFuture<?>> futures = new ArrayList<>();
        futures.add(future1);
        futures.add(future2);
        futures.add(future3);
        futures.add(future4);

        //Make sure the exceptions are first
        latch1.countDown();
        Thread.sleep(1000);
        latch2.countDown();

        String result = (String) util.getAnyOfNonExceptionally(futures, context);
        assertEquals("3",result);
    }

    private Executor getLatchedMockExecutor(CountDownLatch latch) {
        Executor delayedExecutor = Mockito.mock(Executor.class);

        doAnswer(
                (InvocationOnMock invocation) -> {
                    Thread thread = new Thread(() -> {
                        try {
                            latch.await(5, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            throw new RuntimeException();
                        }
                        ((Runnable) invocation.getArguments()[0]).run();
                        return;
                    });
                    thread.start();
                    return null;
                }
        ).when(delayedExecutor).execute(any(Runnable.class));

        return delayedExecutor;
    }
}