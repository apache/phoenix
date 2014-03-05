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
package org.apache.phoenix.hbase.index.parallel;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Threads;

/**
 * Manage access to thread pools
 */
public class ThreadPoolManager {

  private static final Log LOG = LogFactory.getLog(ThreadPoolManager.class);

  /**
   * Get an executor for the given name, based on the passed {@link Configuration}. If a thread pool
   * already exists with that name, it will be returned.
   * @param builder
   * @param env
   * @return a {@link ThreadPoolExecutor} for the given name. Thread pool that only shuts down when
   *         there are no more explicit references to it. You do not need to shutdown the threadpool
   *         on your own - it is managed for you. When you are done, you merely need to release your
   *         reference. If you do attempt to shutdown the pool, you should be careful to call
   *         {@link ThreadPoolExecutor#shutdown()} XOR {@link ThreadPoolExecutor#shutdownNow()} - extra calls to either can lead to
   *         early shutdown of the pool.
   */
  public static synchronized ThreadPoolExecutor getExecutor(ThreadPoolBuilder builder,
      RegionCoprocessorEnvironment env) {
    return getExecutor(builder, env.getSharedData());
  }

  static synchronized ThreadPoolExecutor getExecutor(ThreadPoolBuilder builder,
      Map<String, Object> poolCache) {
    ThreadPoolExecutor pool = (ThreadPoolExecutor) poolCache.get(builder.getName());
    if (pool == null || pool.isTerminating() || pool.isShutdown()) {
      pool = getDefaultExecutor(builder);
      LOG.info("Creating new pool for " + builder.getName());
      poolCache.put(builder.getName(), pool);
    }
    ((ShutdownOnUnusedThreadPoolExecutor) pool).addReference();

    return pool;
  }

  /**
   * @param conf
   */
  private static ShutdownOnUnusedThreadPoolExecutor getDefaultExecutor(ThreadPoolBuilder builder) {
    int maxThreads = builder.getMaxThreads();
    long keepAliveTime = builder.getKeepAliveTime();

    // we prefer starting a new thread to queuing (the opposite of the usual ThreadPoolExecutor)
    // since we are probably writing to a bunch of index tables in this case. Any pending requests
    // are then queued up in an infinite (Integer.MAX_VALUE) queue. However, we allow core threads
    // to timeout, to we tune up/down for bursty situations. We could be a bit smarter and more
    // closely manage the core-thread pool size to handle the bursty traffic (so we can always keep
    // some core threads on hand, rather than starting from scratch each time), but that would take
    // even more time. If we shutdown the pool, but are still putting new tasks, we can just do the
    // usual policy and throw a RejectedExecutionException because we are shutting down anyways and
    // the worst thing is that this gets unloaded.
    ShutdownOnUnusedThreadPoolExecutor pool =
        new ShutdownOnUnusedThreadPoolExecutor(maxThreads, maxThreads, keepAliveTime,
            TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
            Threads.newDaemonThreadFactory(builder.getName() + "-"), builder.getName());
    pool.allowCoreThreadTimeOut(true);
    return pool;
  }

  /**
   * Thread pool that only shuts down when there are no more explicit references to it. A reference
   * is when obtained and released on calls to {@link #shutdown()} or {@link #shutdownNow()}.
   * Therefore, users should be careful to call {@link #shutdown()} XOR {@link #shutdownNow()} -
   * extra calls to either can lead to early shutdown of the pool.
   */
  private static class ShutdownOnUnusedThreadPoolExecutor extends ThreadPoolExecutor {

    private AtomicInteger references;
    private String poolName;

    public ShutdownOnUnusedThreadPoolExecutor(int coreThreads, int maxThreads, long keepAliveTime,
        TimeUnit timeUnit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory,
        String poolName) {
      super(coreThreads, maxThreads, keepAliveTime, timeUnit, workQueue, threadFactory);
      this.references = new AtomicInteger();
      this.poolName = poolName;
    }

    public void addReference() {
      this.references.incrementAndGet();
    }

    @Override
    protected void finalize() {
      // override references counter if we go out of scope - ensures the pool gets cleaned up
      LOG.info("Shutting down pool '" + poolName + "' because no more references");
      super.finalize();
    }

    @Override
    public void shutdown() {
      if (references.decrementAndGet() <= 0) {
        LOG.debug("Shutting down pool " + this.poolName);
        super.shutdown();
      }
    }

    @Override
    public List<Runnable> shutdownNow() {
      if (references.decrementAndGet() <= 0) {
        LOG.debug("Shutting down pool " + this.poolName + " NOW!");
        return super.shutdownNow();
      }
      return Collections.emptyList();
    }

  }
}