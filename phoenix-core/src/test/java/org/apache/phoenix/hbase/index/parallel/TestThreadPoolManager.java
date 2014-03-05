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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;

import org.apache.phoenix.hbase.index.TableName;
import org.apache.phoenix.hbase.index.parallel.ThreadPoolBuilder;
import org.apache.phoenix.hbase.index.parallel.ThreadPoolManager;

public class TestThreadPoolManager {

  @Rule
  public TableName name = new TableName();

  @Test
  public void testShutdownGetsNewThreadPool() throws Exception{
    Map<String, Object> cache = new HashMap<String, Object>();
    ThreadPoolBuilder builder = new ThreadPoolBuilder(name.getTableNameString(), new Configuration(false));
    ThreadPoolExecutor exec = ThreadPoolManager.getExecutor(builder, cache);
    assertNotNull("Got a null exector from the pool!", exec);
    //shutdown the pool and ensure that it actually shutdown
    exec.shutdown();
    ThreadPoolExecutor exec2 = ThreadPoolManager.getExecutor(builder, cache);
    assertFalse("Got the same exectuor, even though the original shutdown", exec2 == exec);
  }

  @Test
  public void testShutdownWithReferencesDoesNotStopExecutor() throws Exception {
    Map<String, Object> cache = new HashMap<String, Object>();
    ThreadPoolBuilder builder =
        new ThreadPoolBuilder(name.getTableNameString(), new Configuration(false));
    ThreadPoolExecutor exec = ThreadPoolManager.getExecutor(builder, cache);
    assertNotNull("Got a null exector from the pool!", exec);
    ThreadPoolExecutor exec2 = ThreadPoolManager.getExecutor(builder, cache);
    assertTrue("Should have gotten the same executor", exec2 == exec);
    exec.shutdown();
    assertFalse("Executor is shutting down, even though we have a live reference!",
      exec.isShutdown() || exec.isTerminating());
    exec2.shutdown();
    // wait 5 minutes for thread pool to shutdown
    assertTrue("Executor is NOT shutting down, after releasing live reference!",
      exec.awaitTermination(300, TimeUnit.SECONDS));
  }

  @Test
  public void testGetExpectedExecutorForName() throws Exception {
    Map<String, Object> cache = new HashMap<String, Object>();
    ThreadPoolBuilder builder =
        new ThreadPoolBuilder(name.getTableNameString(), new Configuration(false));
    ThreadPoolExecutor exec = ThreadPoolManager.getExecutor(builder, cache);
    assertNotNull("Got a null exector from the pool!", exec);
    ThreadPoolExecutor exec2 = ThreadPoolManager.getExecutor(builder, cache);
    assertTrue("Got a different exectuor, even though they have the same name", exec2 == exec);
    builder = new ThreadPoolBuilder(name.getTableNameString(), new Configuration(false));
    exec2 = ThreadPoolManager.getExecutor(builder, cache);
    assertTrue(
      "Got a different exectuor, even though they have the same name, but different confs",
      exec2 == exec);

    builder =
        new ThreadPoolBuilder(name.getTableNameString() + "-some-other-pool", new Configuration(
            false));
    exec2 = ThreadPoolManager.getExecutor(builder, cache);
    assertFalse(
      "Got a different exectuor, even though they have the same name, but different confs",
      exec2 == exec);
  }
}