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
package org.apache.phoenix.coprocessor.tasks;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.coprocessor.TaskRegionObserver.TaskResult;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.task.Task;
import org.apache.phoenix.util.EnvironmentEdgeManager;

import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Runs vector index rebuilds and scorecard reconciles outside the {@code SYSTEM.TASK} sweep. The
 * sweep runs all system tasks one after the other, so long work in the sweep delays other tasks.
 * This runner has one thread for rebuilds and one thread for reconciles. Thus indexes that become
 * due at the same time get their work one after the other, and a rebuild does not delay a
 * reconcile. If the work on an index fails again and again, the runner increases the time between
 * attempts.
 * <p>
 * This region server keeps the work of each task row in memory. It hosts the single
 * {@code SYSTEM.TASK} region. If a task row has no work here, a different region server started
 * that work. That region server then restarted or gave up the region.
 */
final class VectorIndexTaskRunner {

  private static final ExecutorService REBUILDS = Executors.newSingleThreadExecutor(
    new ThreadFactoryBuilder().setNameFormat("vector-index-rebuild-%d").setDaemon(true).build());
  private static final ExecutorService RECONCILIATIONS = Executors.newSingleThreadExecutor(
    new ThreadFactoryBuilder().setNameFormat("vector-index-reconcile-%d").setDaemon(true).build());
  private static final ConcurrentHashMap<String, Future<TaskResult>> RUNNING =
    new ConcurrentHashMap<>();
  /** For each index, the number of failures in sequence and the earliest time of a retry. */
  private static final ConcurrentHashMap<String, long[]> FAILURES = new ConcurrentHashMap<>();

  private VectorIndexTaskRunner() {
  }

  private static String key(Task.TaskRecord task) {
    return task.getTaskType() + "/" + task.getTenantId() + "/" + task.getSchemaName() + "/"
      + task.getTableName() + "/" + task.getTimeStamp().getTime();
  }

  /**
   * Submits the work of a task row. If this region server already has work for that row, this call
   * does nothing.
   */
  static void submit(Task.TaskRecord task, Callable<TaskResult> work) {
    ExecutorService executor =
      task.getTaskType() == PTable.TaskType.VECTOR_INDEX_REBUILD ? REBUILDS : RECONCILIATIONS;
    RUNNING.computeIfAbsent(key(task), k -> executor.submit(work));
  }

  /** Returns the work of a task row, or null if this region server did not submit it. */
  static Future<TaskResult> get(Task.TaskRecord task) {
    return RUNNING.get(key(task));
  }

  /** Removes the finished work of a task row from memory. */
  static void remove(Task.TaskRecord task, Future<TaskResult> work) {
    RUNNING.remove(key(task), work);
  }

  /** Returns true if failures delay the next attempt of the work on an index. */
  static boolean isBackingOff(String indexName) {
    long[] failures = FAILURES.get(indexName);
    return failures != null && EnvironmentEdgeManager.currentTimeMillis() < failures[1];
  }

  /**
   * Records a failure of the work on an index. The first retry delay is the sweep interval. Each
   * failure in sequence doubles the delay, up to the reconcile interval. Thus a scan or migration
   * that always fails does not run again on each sweep.
   */
  static void failed(String indexName, Configuration conf) {
    long sweep = conf.getLong(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB,
      QueryServicesOptions.DEFAULT_TASK_HANDLING_INTERVAL_MS);
    long max = conf.getLong(QueryServices.VECTOR_SCORECARD_RECONCILE_INTERVAL_MS_ATTRIB,
      QueryServicesOptions.DEFAULT_VECTOR_SCORECARD_RECONCILE_INTERVAL_MS);
    long now = EnvironmentEdgeManager.currentTimeMillis();
    FAILURES.compute(indexName, (k, failures) -> {
      long count = failures == null ? 1 : failures[0] + 1;
      return new long[] { count, now + backoff(count, sweep, max) };
    });
  }

  /** Clears the failures of an index after its work succeeds. */
  static void succeeded(String indexName) {
    FAILURES.remove(indexName);
  }

  /** Returns the delay before the next attempt after {@code failures} failures in sequence. */
  static long backoff(long failures, long sweep, long max) {
    return Math.min(max, sweep << Math.min(failures - 1, 30));
  }
}
