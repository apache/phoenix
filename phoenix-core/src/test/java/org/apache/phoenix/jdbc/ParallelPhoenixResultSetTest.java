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

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

public class ParallelPhoenixResultSetTest {
    CompletableFuture<ResultSet> completableRs1;
    CompletableFuture<ResultSet> completableRs2;

    ParallelPhoenixResultSet resultSet;

    @Before
    public void init() {
        completableRs1 = Mockito.mock(CompletableFuture.class);
        completableRs2 = Mockito.mock(CompletableFuture.class);
        resultSet =
                new ParallelPhoenixResultSet(
                        new ParallelPhoenixContext(new Properties(), null,
                                HighAvailabilityTestingUtility
                                        .getListOfSingleThreadExecutorServices(),
                                null),
                        completableRs1, completableRs2);
    }

    @Test
    public void testUnbound() throws SQLException {
        ResultSet rs = resultSet.getResultSet();
        assertNull(rs);
    }

    @Test
    public void testNextBound() throws SQLException {
        ResultSet rs = Mockito.mock(ResultSet.class);
        resultSet.setResultSet(rs);
        resultSet.next();
        Mockito.verify(rs).next();
        Mockito.verifyNoMoreInteractions(rs);
    }

    @Test
    public void testRS1WinsNext() throws Exception {

        ResultSet rs1 = Mockito.mock(ResultSet.class);
        ResultSet rs2 = Mockito.mock(ResultSet.class);

        Executor rsExecutor2 = Mockito.mock(Executor.class);

        CountDownLatch latch = new CountDownLatch(1);

        //inject a sleep
        doAnswer(
                (InvocationOnMock invocation) -> {
                    Thread thread = new Thread(() -> {
                        try {
                            //TODO: Remove this sleep
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
        ).when(rsExecutor2).execute(any(Runnable.class));

        completableRs1 = CompletableFuture.completedFuture(rs1);

        completableRs2 = CompletableFuture.supplyAsync(() -> rs2, rsExecutor2);

        resultSet =
                new ParallelPhoenixResultSet(
                        new ParallelPhoenixContext(new Properties(), null,
                                HighAvailabilityTestingUtility
                                        .getListOfSingleThreadExecutorServices(),
                                null),
                        completableRs1, completableRs2);

        resultSet.next();

        assertEquals(rs1, resultSet.getResultSet());
    }

    @Test
    public void testRS2WinsNext() throws Exception {
        ResultSet rs1 = Mockito.mock(ResultSet.class);
        ResultSet rs2 = Mockito.mock(ResultSet.class);

        Executor rsExecutor1 = Mockito.mock(Executor.class);
        CountDownLatch latch = new CountDownLatch(1);
        //inject a sleep
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
        ).when(rsExecutor1).execute(any(Runnable.class));

        completableRs1 = CompletableFuture.supplyAsync(() -> rs1, rsExecutor1);
        completableRs2 = CompletableFuture.completedFuture(rs2);

        resultSet =
                new ParallelPhoenixResultSet(
                        new ParallelPhoenixContext(new Properties(), null,
                                HighAvailabilityTestingUtility
                                        .getListOfSingleThreadExecutorServices(),
                                null),
                        completableRs1, completableRs2);

        resultSet.next();

        assertEquals(rs2, resultSet.getResultSet());
    }

    @Test
    public void testRS1FailsImmediatelyNext() throws Exception {
        ResultSet rs2 = Mockito.mock(ResultSet.class);
        Executor rsExecutor2 = Mockito.mock(Executor.class);
        CountDownLatch latch = new CountDownLatch(1);
        //inject a sleep
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
        ).when(rsExecutor2).execute(any(Runnable.class));

        completableRs1 = new CompletableFuture<>();
        completableRs1.completeExceptionally(new RuntimeException("Failure"));

        completableRs2 = CompletableFuture.supplyAsync(() -> rs2, rsExecutor2);

        resultSet =
                new ParallelPhoenixResultSet(
                        new ParallelPhoenixContext(new Properties(), null,
                                HighAvailabilityTestingUtility
                                        .getListOfSingleThreadExecutorServices(),
                                null),
                        completableRs1, completableRs2);

        resultSet.next();

        assertEquals(rs2, resultSet.getResultSet());
    }

    @Test
    public void testRS1SucceedsDuringNext() throws Exception {
        ResultSet rs1 = Mockito.mock(ResultSet.class);
        ResultSet rs2 = Mockito.mock(ResultSet.class);

        Executor rsExecutor1 = Mockito.mock(Executor.class);
        Executor rsExecutor2 = Mockito.mock(Executor.class);
        CountDownLatch latch0 = new CountDownLatch(1);
        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        CountDownLatch latch3 = new CountDownLatch(1);
        //inject a sleep
        doAnswer(
                (InvocationOnMock invocation) -> {
                    Thread thread = new Thread(() -> {
                        try {
                            latch1.await(10, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            throw new RuntimeException();
                        }
                        ((Runnable) invocation.getArguments()[0]).run();
                        return;
                    });
                    thread.start();
                    return null;
                }
        ).when(rsExecutor1).execute(any(Runnable.class));

        //inject a sleep
        doAnswer(
                (InvocationOnMock invocation) -> {
                    Thread thread = new Thread(() -> {
                        try {
                            latch2.await(10, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            throw new RuntimeException();
                        }
                        ((Runnable) invocation.getArguments()[0]).run();
                        return;
                    });
                    thread.start();
                    return null;
                }
        ).when(rsExecutor2).execute(any(Runnable.class));

        completableRs1 = CompletableFuture.supplyAsync(() -> rs1, rsExecutor1);
        completableRs2 = CompletableFuture.supplyAsync(() -> rs2, rsExecutor2);

        resultSet =
                new ParallelPhoenixResultSet(
                        new ParallelPhoenixContext(new Properties(), null,
                                HighAvailabilityTestingUtility
                                        .getListOfSingleThreadExecutorServices(),
                                null),
                        completableRs1, completableRs2);

        //run next in the background
        ExecutorService testService = Executors.newSingleThreadExecutor();
        testService.execute(() -> {
            try {
                latch0.countDown();
                resultSet.next();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } finally {
                latch3.countDown();
            }
        });

        //Wait for next to start
        latch0.await(10, TimeUnit.SECONDS);

        //Start RS1 asynch
        latch1.countDown();

        //Wait for next to finish
        latch3.await(10, TimeUnit.SECONDS);

        assertEquals(rs1, resultSet.getResultSet());

        //Cleanup
        latch2.countDown();
    }
}