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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;

/**
 * A group of {@link Task}s. The tasks are all bound together using the same {@link Abortable} (
 * <tt>this</tt>) to ensure that all tasks are aware when any of the other tasks fails.
 * @param <V> expected result type from all the tasks
 */
public class TaskBatch<V> implements Abortable {
  private static final Log LOG = LogFactory.getLog(TaskBatch.class);
  private AtomicBoolean aborted = new AtomicBoolean();
  private List<Task<V>> tasks;

  /**
   * @param size expected number of tasks
   */
  public TaskBatch(int size) {
    this.tasks = new ArrayList<Task<V>>(size);
  }

  public void add(Task<V> task) {
    this.tasks.add(task);
    task.setBatchMonitor(this);
  }

  public Collection<Task<V>> getTasks() {
    return this.tasks;
  }

  @Override
  public void abort(String why, Throwable e) {
    if (this.aborted.getAndSet(true)) {
      return;
    }
    LOG.info("Aborting batch of tasks because " + why);
  }

  @Override
  public boolean isAborted() {
    return this.aborted.get();
  }

  /**
   * @return the number of tasks assigned to this batch
   */
  public int size() {
    return this.tasks.size();
  }
}