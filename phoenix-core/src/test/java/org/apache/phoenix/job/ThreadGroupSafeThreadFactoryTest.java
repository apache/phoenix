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
package org.apache.phoenix.job;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assume.assumeTrue;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class ThreadGroupSafeThreadFactoryTest {

  /**
   * Runs the callable in a new thread that belongs to the given group, and returns its result.
   */
  private static <T> T runIn(ThreadGroup group, Callable<T> callable) throws Exception {
    AtomicReference<T> result = new AtomicReference<>();
    AtomicReference<Throwable> error = new AtomicReference<>();
    Thread creator = new Thread(group, () -> {
      try {
        result.set(callable.call());
      } catch (Throwable t) {
        error.set(t);
      }
    }, "creator");
    creator.start();
    creator.join();
    if (error.get() != null) {
      throw new AssertionError(error.get());
    }
    return result.get();
  }

  @Test
  public void testUsesCreatorThreadGroup() throws Exception {
    ThreadGroup creatorGroup = new ThreadGroup("ThreadGroupSafeThreadFactoryTest-creator");
    ThreadFactory factory = runIn(creatorGroup, ThreadGroupSafeThreadFactory::new);
    Thread caller = Thread.currentThread();
    int callerPriority = caller.getPriority();
    caller.setPriority(Thread.MIN_PRIORITY);
    try {
      Thread t = factory.newThread(() -> {
      });
      assertSame(creatorGroup, t.getThreadGroup());
      assertFalse(t.isDaemon());
      assertEquals(Thread.NORM_PRIORITY, t.getPriority());
    } finally {
      caller.setPriority(callerPriority);
      creatorGroup.destroy();
    }
  }

  @Test
  public void testFallsBackToCallerThreadGroupWhenCreatorGroupIsDestroyed() throws Exception {
    ThreadGroup creatorGroup = new ThreadGroup("ThreadGroupSafeThreadFactoryTest-creator");
    ThreadFactory factory = runIn(creatorGroup, ThreadGroupSafeThreadFactory::new);
    creatorGroup.destroy();
    // ThreadGroup.destroy() is a no-op on JDK 19+, so the problem cannot occur there.
    assumeTrue(creatorGroup.isDestroyed());

    Thread t = factory.newThread(() -> {
    });
    assertSame(Thread.currentThread().getThreadGroup(), t.getThreadGroup());
    assertFalse(t.isDaemon());
    assertEquals(Thread.NORM_PRIORITY, t.getPriority());
  }

  @Test
  public void testJobManagerExecutorSurvivesDestroyedCreatorThreadGroup() throws Exception {
    ThreadGroup creatorGroup = new ThreadGroup("ThreadGroupSafeThreadFactoryTest-creator");
    ThreadPoolExecutor exec = runIn(creatorGroup, () -> {
      ThreadPoolExecutor e = JobManager.createThreadPoolExec(10, 2, 10, false);
      e.submit(() -> {
      }).get();
      return e;
    });
    try {
      // Wait for the idle core threads to time out so the next task needs a new worker.
      long deadline = System.currentTimeMillis() + 30000;
      while (exec.getPoolSize() > 0 && System.currentTimeMillis() < deadline) {
        Thread.sleep(10);
      }
      assertEquals(0, exec.getPoolSize());
      while (creatorGroup.activeCount() > 0 && System.currentTimeMillis() < deadline) {
        Thread.sleep(10);
      }
      creatorGroup.destroy();
      // ThreadGroup.destroy() is a no-op on JDK 19+, so the problem cannot occur there.
      assumeTrue(creatorGroup.isDestroyed());

      assertEquals("ok", exec.submit(() -> "ok").get(30, TimeUnit.SECONDS));
    } finally {
      exec.shutdownNow();
    }
  }
}
