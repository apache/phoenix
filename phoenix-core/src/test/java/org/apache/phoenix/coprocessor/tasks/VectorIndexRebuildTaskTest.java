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

import static org.apache.phoenix.coprocessor.tasks.VectorIndexTaskRunnerTest.task;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.phoenix.coprocessor.TaskRegionObserver.TaskResult;
import org.apache.phoenix.coprocessor.TaskRegionObserver.TaskResultCode;
import org.apache.phoenix.schema.task.Task;
import org.junit.Test;

/** Verifies how sweeps poll a rebuild that runs outside the sweep. */
public class VectorIndexRebuildTaskTest {

  @Test
  public void testLostRebuildRunsAgain() throws Exception {
    // A STARTED row from a RegionServer that restarted or released SYSTEM.TASK
    Task.TaskRecord task = task("IDX_LOST", 1L);
    assertNull("Polled again on the next sweep",
      new VectorIndexRebuildTask().checkCurrentResult(task));
    assertNotNull("Submitted here", VectorIndexTaskRunner.get(task));
  }

  @Test
  public void testRunningRebuildIsPolledUntilDone() throws Exception {
    Task.TaskRecord task = task("IDX_POLL", 1L);
    CountDownLatch release = new CountDownLatch(1);
    VectorIndexTaskRunner.submit(task, () -> {
      release.await(30, TimeUnit.SECONDS);
      return new TaskResult(TaskResultCode.SUCCESS, "REBUILT");
    });
    VectorIndexRebuildTask rebuild = new VectorIndexRebuildTask();
    assertNull("Still running", rebuild.checkCurrentResult(task));
    release.countDown();
    VectorIndexTaskRunner.get(task).get(30, TimeUnit.SECONDS);
    TaskResult result = rebuild.checkCurrentResult(task);
    assertEquals(TaskResultCode.SUCCESS, result.getResultCode());
    assertEquals("REBUILT", result.getDetails());
    assertNull("The finished rebuild is forgotten", VectorIndexTaskRunner.get(task));
  }

  @Test
  public void testBusyRebuildRunsAgain() throws Exception {
    Task.TaskRecord task = task("IDX_BUSY", 1L);
    VectorIndexTaskRunner.submit(task,
      () -> new TaskResult(TaskResultCode.SKIPPED, "Vector index IDX_BUSY is busy"));
    Future<TaskResult> busy = VectorIndexTaskRunner.get(task);
    busy.get(30, TimeUnit.SECONDS);
    assertNull("Polled again on the next sweep",
      new VectorIndexRebuildTask().checkCurrentResult(task));
    Future<TaskResult> again = VectorIndexTaskRunner.get(task);
    assertNotSame(busy, again);
    assertSame(again, VectorIndexTaskRunner.get(task));
  }
}
