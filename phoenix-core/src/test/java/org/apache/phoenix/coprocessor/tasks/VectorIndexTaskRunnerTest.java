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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Timestamp;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.coprocessor.TaskRegionObserver.TaskResult;
import org.apache.phoenix.coprocessor.TaskRegionObserver.TaskResultCode;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.task.Task;
import org.junit.Test;

public class VectorIndexTaskRunnerTest {

  private static final long MINUTE = 60_000L;
  private static final long DAY = 24 * 60 * MINUTE;

  static Task.TaskRecord task(String table, long ts) {
    Task.TaskRecord task = new Task.TaskRecord();
    task.setTaskType(PTable.TaskType.VECTOR_INDEX_REBUILD);
    task.setTableName(table);
    task.setTimeStamp(new Timestamp(ts));
    return task;
  }

  @Test
  public void testBackoffDoublesFromTheSweepIntervalUpToTheCap() {
    assertEquals(MINUTE, VectorIndexTaskRunner.backoff(1, MINUTE, DAY));
    assertEquals(2 * MINUTE, VectorIndexTaskRunner.backoff(2, MINUTE, DAY));
    assertEquals(512 * MINUTE, VectorIndexTaskRunner.backoff(10, MINUTE, DAY));
    assertEquals(1024 * MINUTE, VectorIndexTaskRunner.backoff(11, MINUTE, DAY));
    assertEquals(DAY, VectorIndexTaskRunner.backoff(12, MINUTE, DAY));
    assertEquals("No overflow after many failures", DAY,
      VectorIndexTaskRunner.backoff(1000, MINUTE, DAY));
    assertEquals(0, VectorIndexTaskRunner.backoff(3, MINUTE, 0));
  }

  @Test
  public void testFailuresDeferTheIndexUntilItSucceeds() {
    Configuration conf = new Configuration(false);
    conf.setLong(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB, MINUTE);
    conf.setLong(QueryServices.VECTOR_SCORECARD_RECONCILE_INTERVAL_MS_ATTRIB, DAY);
    String index = "IDX_BACKOFF";
    assertFalse(VectorIndexTaskRunner.isBackingOff(index));
    VectorIndexTaskRunner.failed(index, conf);
    assertTrue(VectorIndexTaskRunner.isBackingOff(index));
    assertFalse("Other indexes are unaffected",
      VectorIndexTaskRunner.isBackingOff(index + "_OTHER"));
    VectorIndexTaskRunner.succeeded(index);
    assertFalse(VectorIndexTaskRunner.isBackingOff(index));
  }

  @Test
  public void testZeroReconcileIntervalDoesNotDefer() {
    Configuration conf = new Configuration(false);
    conf.setLong(QueryServices.VECTOR_SCORECARD_RECONCILE_INTERVAL_MS_ATTRIB, 0);
    VectorIndexTaskRunner.failed("IDX_NO_BACKOFF", conf);
    assertFalse(VectorIndexTaskRunner.isBackingOff("IDX_NO_BACKOFF"));
  }

  @Test
  public void testATaskRowRunsItsWorkOnce() throws Exception {
    Task.TaskRecord task = task("IDX_ONCE", 1L);
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger runs = new AtomicInteger();
    VectorIndexTaskRunner.submit(task, () -> {
      runs.incrementAndGet();
      release.await(30, TimeUnit.SECONDS);
      return new TaskResult(TaskResultCode.SUCCESS, "");
    });
    Future<TaskResult> work = VectorIndexTaskRunner.get(task);
    VectorIndexTaskRunner.submit(task, () -> {
      runs.incrementAndGet();
      return new TaskResult(TaskResultCode.SUCCESS, "");
    });
    assertTrue(work == VectorIndexTaskRunner.get(task));
    assertFalse("Another row of the index is separate work",
      VectorIndexTaskRunner.get(task("IDX_ONCE", 2L)) != null);
    release.countDown();
    assertEquals(TaskResultCode.SUCCESS, work.get(30, TimeUnit.SECONDS).getResultCode());
    assertEquals(1, runs.get());
    VectorIndexTaskRunner.remove(task, work);
    assertTrue(VectorIndexTaskRunner.get(task) == null);
  }

  @Test
  public void testRebuildsRunOneAtATimeBesideReconciliations() throws Exception {
    CountDownLatch firstRunning = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch secondRunning = new CountDownLatch(1);
    Task.TaskRecord first = task("IDX_SERIAL_A", 1L);
    Task.TaskRecord second = task("IDX_SERIAL_B", 1L);
    Task.TaskRecord reconcile = task("IDX_SERIAL_C", 1L);
    reconcile.setTaskType(PTable.TaskType.VECTOR_SCORECARD_RECONCILE);
    try {
      VectorIndexTaskRunner.submit(first, () -> {
        firstRunning.countDown();
        release.await(30, TimeUnit.SECONDS);
        return new TaskResult(TaskResultCode.SUCCESS, "");
      });
      assertTrue(firstRunning.await(30, TimeUnit.SECONDS));
      VectorIndexTaskRunner.submit(second, () -> {
        secondRunning.countDown();
        return new TaskResult(TaskResultCode.SUCCESS, "");
      });
      VectorIndexTaskRunner.submit(reconcile, () -> new TaskResult(TaskResultCode.SUCCESS, ""));
      assertEquals("A reconciliation runs while a rebuild does", TaskResultCode.SUCCESS,
        VectorIndexTaskRunner.get(reconcile).get(30, TimeUnit.SECONDS).getResultCode());
      assertFalse("A second rebuild waits for the first",
        secondRunning.await(500, TimeUnit.MILLISECONDS));
      release.countDown();
      assertTrue(secondRunning.await(30, TimeUnit.SECONDS));
    } finally {
      release.countDown();
      for (Task.TaskRecord task : new Task.TaskRecord[] { first, second, reconcile }) {
        Future<TaskResult> work = VectorIndexTaskRunner.get(task);
        if (work != null) {
          work.get(30, TimeUnit.SECONDS);
          VectorIndexTaskRunner.remove(task, work);
        }
      }
    }
  }
}
