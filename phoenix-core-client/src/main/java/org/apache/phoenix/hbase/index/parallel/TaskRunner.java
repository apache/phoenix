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
package org.apache.phoenix.hbase.index.parallel;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.util.Pair;

/**
 *
 */
public interface TaskRunner extends Stoppable {

  /**
   * Submit the given tasks to the pool and wait for them to complete. fail.
   * <p>
   * Non-interruptible method. To stop any running tasks call {@link #stop(String)} - this will
   * shutdown the thread pool, causing any pending tasks to be failed early (whose failure will be
   * ignored) and interrupt any running tasks. It is up to the passed tasks to respect the interrupt
   * notification
   * @param tasks to run
   * @return Pair containing ordered List of results from each task and an ordered immutable list of
   *         underlying futures which can be used for getting underlying exceptions
   * @throws ExecutionException   if any of the tasks fails. Wraps the underyling failure, which can
   *                              be retrieved via {@link ExecutionException#getCause()}.
   * @throws InterruptedException if the current thread is interrupted while waiting for the batch
   *                              to complete
   */
  public <R> Pair<List<R>, List<Future<R>>> submit(TaskBatch<R> tasks)
    throws ExecutionException, InterruptedException;

  /**
   * Similar to {@link #submit(TaskBatch)}, but is not interruptible. If an interrupt is found while
   * waiting for results, we ignore it and only stop is {@link #stop(String)} has been called. On
   * return from the method, the interrupt status of the thread is restored.
   * @param tasks to run
   * @return Pair containing ordered List of results from each task and an ordered immutable list of
   *         underlying futures which can be used for getting underlying exceptions
   * @throws EarlyExitFailure   if there are still tasks to submit to the pool, but there is a stop
   *                            notification
   * @throws ExecutionException if any of the tasks fails. Wraps the underyling failure, which can
   *                            be retrieved via {@link ExecutionException#getCause()}.
   */
  public <R> Pair<List<R>, List<Future<R>>> submitUninterruptible(TaskBatch<R> tasks)
    throws EarlyExitFailure, ExecutionException;

  /**
   * Submit the given tasks to the pool without waiting for them to complete or collecting results.
   * This is a fire-and-forget operation that allows tasks to run asynchronously in the background.
   * <p>
   * Unlike {@link #submit(TaskBatch)} and {@link #submitUninterruptible(TaskBatch)}, this method
   * does not block waiting for task completion and does not return results or futures. It is useful
   * for scenarios where you want to initiate background processing but don't need to wait for or
   * collect the results.
   * <p>
   * Tasks are submitted to the underlying thread pool and will execute according to the pool's
   * scheduling policy. If any task fails during execution, the failure will be handled internally
   * and will not propagate back to the caller since no results are collected.
   * @param <R>   the type of result that would be returned by the tasks (unused since no results
   *              are collected)
   * @param tasks the batch of tasks to submit for asynchronous execution
   * @throws ExecutionException if there is an error submitting the tasks to the thread pool
   */
  <R> void submitOnly(TaskBatch<R> tasks) throws ExecutionException;
}
