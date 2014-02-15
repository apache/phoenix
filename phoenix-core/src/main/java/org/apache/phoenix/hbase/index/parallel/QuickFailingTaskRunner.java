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
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * {@link TaskRunner} that attempts to run all tasks passed, but quits early if any {@link Task}
 * fails, not waiting for the remaining {@link Task}s to complete.
 */
public class QuickFailingTaskRunner extends BaseTaskRunner {

  static final Log LOG = LogFactory.getLog(QuickFailingTaskRunner.class);

  /**
   * @param service thread pool to which {@link Task}s are submitted. This service is then 'owned'
   *          by <tt>this</tt> and will be shutdown on calls to {@link #stop(String)}.
   */
  public QuickFailingTaskRunner(ExecutorService service) {
    super(service);
  }

  @Override
  protected <R> ListenableFuture<List<R>> submitTasks(List<ListenableFuture<R>> futures) {
    return Futures.allAsList(futures);
  }
}