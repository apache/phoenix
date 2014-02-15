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

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * A {@link TaskRunner} that ensures that all the tasks have been attempted before we return, even
 * if some of the tasks cause failures.
 * <p>
 * Because we wait until the entire batch is complete to see the failure, checking for failure of
 * the {@link TaskBatch} on the submitted tasks will not help - they will never see the failure of
 * the other tasks. You will need to provide an external mechanism to propagate the error.
 * <p>
 * Does not throw an {@link ExecutionException} if any of the tasks fail.
 */
public class WaitForCompletionTaskRunner extends BaseTaskRunner {
  
  /**
   * @param service thread pool to which {@link Task}s are submitted. This service is then 'owned'
   *          by <tt>this</tt> and will be shutdown on calls to {@link #stop(String)}.
   */
  public WaitForCompletionTaskRunner(ExecutorService service) {
    super(service);
  }

  @Override
  public <R> ListenableFuture<List<R>> submitTasks(List<ListenableFuture<R>> futures) {
    return Futures.successfulAsList(futures);
  }
}