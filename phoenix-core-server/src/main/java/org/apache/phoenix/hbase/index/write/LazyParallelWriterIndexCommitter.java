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
package org.apache.phoenix.hbase.index.write;

import org.apache.phoenix.hbase.index.exception.SingleIndexWriteFailureException;
import org.apache.phoenix.hbase.index.parallel.TaskBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Like the {@link ParallelWriterIndexCommitter}, but does not block
 */
public class LazyParallelWriterIndexCommitter extends AbstractParallelWriterIndexCommitter {

  private static final Logger LOGGER =
    LoggerFactory.getLogger(LazyParallelWriterIndexCommitter.class);

  public LazyParallelWriterIndexCommitter() {
    super();
  }

  @Override
  protected void submitTasks(TaskBatch<Void> tasks) throws SingleIndexWriteFailureException {
    try {
      pool.submitOnly(tasks);
    } catch (Exception e) {
      LOGGER.error("Error while submitting the task.", e);
      propagateFailure(e.getCause());
    }
  }

}
