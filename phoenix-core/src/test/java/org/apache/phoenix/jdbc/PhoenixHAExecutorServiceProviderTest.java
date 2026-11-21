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
package org.apache.phoenix.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class PhoenixHAExecutorServiceProviderTest {

  private static final Properties properties = new Properties();

  @BeforeClass
  public static void setupBeforeClass() {
    properties.setProperty(PhoenixHAExecutorServiceProvider.HA_MAX_POOL_SIZE, "2");
    properties.setProperty(PhoenixHAExecutorServiceProvider.HA_MAX_QUEUE_SIZE, "5");
  }

  @AfterClass
  public static void afterClass() {
    PhoenixHAExecutorServiceProvider.resetExecutor();
  }

  @Before
  public void beforeTest() {
    PhoenixHAExecutorServiceProvider.resetExecutor();
    PhoenixHAExecutorServiceProvider.get(properties);
  }

  @After
  public void afterTest() {
    for (PhoenixHAExecutorServiceProvider.PhoenixHAClusterExecutorServices c : PhoenixHAExecutorServiceProvider
      .get(properties)) {
      c.getExecutorService().shutdownNow();
      c.getCloseExecutorService().shutdownNow();
    }
  }

  @Test
  public void testHAExecutorService1Capacity() {
    testHAExecutorServiceCapacity(0);
  }

  @Test
  public void testHAExecutorService2Capacity() {
    testHAExecutorServiceCapacity(1);
  }

  private void testHAExecutorServiceCapacity(int index) {
    Properties props = new Properties();
    props.setProperty(PhoenixHAExecutorServiceProvider.HA_THREADPOOL_QUEUE_BACKOFF_THRESHOLD,
      "0.5");
    ThreadPoolExecutor es = (ThreadPoolExecutor) PhoenixHAExecutorServiceProvider.get(properties)
      .get(index).getExecutorService();
    Object obj = new Object();
    CompletableFuture.runAsync(getWaitingRunnable(obj), es);
    CompletableFuture.runAsync(getWaitingRunnable(obj), es);
    assertEquals(es.getQueue().size(), 0);
    CompletableFuture.runAsync(getWaitingRunnable(obj), es);
    CompletableFuture.runAsync(getWaitingRunnable(obj), es);
    assertTrue(PhoenixHAExecutorServiceProvider.hasCapacity(props).get(index));
    CompletableFuture.runAsync(getWaitingRunnable(obj), es);
    assertFalse(PhoenixHAExecutorServiceProvider.hasCapacity(props).get(index));
    synchronized (obj) {
      obj.notifyAll();
    }
  }

  @Test
  public void testHAExecutorServiceQueuing() {
    ThreadPoolExecutor es = (ThreadPoolExecutor) PhoenixHAExecutorServiceProvider.get(properties)
      .get(0).getExecutorService();
    Object obj = new Object();
    CompletableFuture.runAsync(getWaitingRunnable(obj), es);
    assertEquals(es.getQueue().size(), 0);
    CompletableFuture.runAsync(getWaitingRunnable(obj), es);
    assertEquals(es.getQueue().size(), 0);
    CompletableFuture.runAsync(getWaitingRunnable(obj), es);
    assertEquals(es.getQueue().size(), 1);
    synchronized (obj) {
      obj.notifyAll();
    }
  }

  @Test
  public void testHAExecutorServiceCloserConfigured() {
    ThreadPoolExecutor es1 = (ThreadPoolExecutor) PhoenixHAExecutorServiceProvider.get(properties)
      .get(0).getCloseExecutorService();
    ThreadPoolExecutor es2 = (ThreadPoolExecutor) PhoenixHAExecutorServiceProvider.get(properties)
      .get(1).getCloseExecutorService();
    int expectedPoolSize =
      Integer.valueOf(PhoenixHAExecutorServiceProvider.DEFAULT_HA_CLOSE_MAX_POOL_SIZE);
    assertEquals(expectedPoolSize, es1.getMaximumPoolSize());
    assertEquals(expectedPoolSize, es2.getMaximumPoolSize());
    assertNotEquals(es1, es2);
  }

  @Test
  public void testHAExecutorServiceSurvivesDestroyedCreatorThreadGroup() throws Exception {
    ThreadGroup creatorGroup = new ThreadGroup("PhoenixHAExecutorServiceProviderTest-creator");
    AtomicReference<Throwable> creatorError = new AtomicReference<>();
    Thread creator = new Thread(creatorGroup, () -> {
      try {
        PhoenixHAExecutorServiceProvider.resetExecutor();
        for (PhoenixHAExecutorServiceProvider.PhoenixHAClusterExecutorServices c : PhoenixHAExecutorServiceProvider
          .get(properties)) {
          for (ExecutorService es : Arrays.asList(c.getExecutorService(),
            c.getCloseExecutorService())) {
            ((ThreadPoolExecutor) es).setKeepAliveTime(10, TimeUnit.MILLISECONDS);
            es.submit(() -> {
            }).get();
          }
        }
      } catch (Throwable t) {
        creatorError.set(t);
      }
    }, "PhoenixHAExecutorServiceProviderTest-creator");
    creator.start();
    creator.join();
    if (creatorError.get() != null) {
      throw new AssertionError(creatorError.get());
    }

    // Wait for the idle core threads to time out so the next task needs a new worker.
    long deadline = System.currentTimeMillis() + 30000;
    while (creatorGroup.activeCount() > 0 && System.currentTimeMillis() < deadline) {
      Thread.sleep(10);
    }
    creatorGroup.destroy();
    // ThreadGroup.destroy() is a no-op on JDK 19+, so the problem cannot occur there.
    assumeTrue(creatorGroup.isDestroyed());

    for (PhoenixHAExecutorServiceProvider.PhoenixHAClusterExecutorServices c : PhoenixHAExecutorServiceProvider
      .get(properties)) {
      for (ExecutorService es : Arrays.asList(c.getExecutorService(),
        c.getCloseExecutorService())) {
        assertEquals("ok", es.submit(() -> "ok").get(30, TimeUnit.SECONDS));
      }
    }
  }

  private Runnable getWaitingRunnable(Object obj) {
    return (() -> {
      synchronized (obj) {
        try {
          obj.wait();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }
}
