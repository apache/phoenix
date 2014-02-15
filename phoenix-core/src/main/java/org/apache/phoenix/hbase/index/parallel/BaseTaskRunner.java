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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * {@link TaskRunner} that just manages the underlying thread pool. On called to
 * {@link #stop(String)}, the thread pool is shutdown immediately - all pending tasks are cancelled
 * and running tasks receive and interrupt.
 * <p>
 * If we find a failure the failure is propagated to the {@link TaskBatch} so any {@link Task} that
 * is interested can kill itself as well.
 */
public abstract class BaseTaskRunner implements TaskRunner {

  private static final Log LOG = LogFactory.getLog(BaseTaskRunner.class);
  protected ListeningExecutorService writerPool;
  private boolean stopped;

  public BaseTaskRunner(ExecutorService service) {
    this.writerPool = MoreExecutors.listeningDecorator(service);
  }

  @Override
  public <R> List<R> submit(TaskBatch<R> tasks) throws CancellationException, ExecutionException,
      InterruptedException {
    // submit each task to the pool and queue it up to be watched
    List<ListenableFuture<R>> futures = new ArrayList<ListenableFuture<R>>(tasks.size());
    for (Task<R> task : tasks.getTasks()) {
      futures.add(this.writerPool.submit(task));
    }
    try {
      // This logic is actually much more synchronized than the previous logic. Now we rely on a
      // synchronization around the status to tell us when we are done. While this does have the
      // advantage of being (1) less code, and (2) supported as part of a library, it is just that
      // little bit slower. If push comes to shove, we can revert back to the previous
      // implementation, but for right now, this works just fine.
      return submitTasks(futures).get();
    } catch (CancellationException e) {
      // propagate the failure back out
      logAndNotifyAbort(e, tasks);
      throw e;
    } catch (ExecutionException e) {
      // propagate the failure back out
      logAndNotifyAbort(e, tasks);
      throw e;
    }
  }

  private void logAndNotifyAbort(Exception e, Abortable abort) {
    String msg = "Found a failed task because: " + e.getMessage();
    LOG.error(msg, e);
    abort.abort(msg, e.getCause());
  }

  /**
   * Build a ListenableFuture for the tasks. Implementing classes can determine return behaviors on
   * the given tasks
   * @param futures to wait on
   * @return a single {@link ListenableFuture} that completes based on the passes tasks.
   */
  protected abstract <R> ListenableFuture<List<R>> submitTasks(List<ListenableFuture<R>> futures);

  @Override
  public <R> List<R> submitUninterruptible(TaskBatch<R> tasks) throws EarlyExitFailure,
      ExecutionException {
    boolean interrupted = false;
    try {
      while (!this.isStopped()) {
        try {
          return this.submit(tasks);
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      // restore the interrupted status
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }

    // should only get here if we are interrupted while waiting for a result and have been told to
    // shutdown by an external source
    throw new EarlyExitFailure("Interrupted and stopped before computation was complete!");
  }

  @Override
  public void stop(String why) {
    if (this.stopped) {
      return;
    }
    LOG.info("Shutting down task runner because " + why);
    this.writerPool.shutdownNow();
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }
}