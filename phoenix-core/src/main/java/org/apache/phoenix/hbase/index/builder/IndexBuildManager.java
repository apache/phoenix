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
package org.apache.phoenix.hbase.index.builder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.hbase.index.Indexer;
import org.apache.phoenix.hbase.index.parallel.QuickFailingTaskRunner;
import org.apache.phoenix.hbase.index.parallel.Task;
import org.apache.phoenix.hbase.index.parallel.TaskBatch;
import org.apache.phoenix.hbase.index.parallel.ThreadPoolBuilder;
import org.apache.phoenix.hbase.index.parallel.ThreadPoolManager;

/**
 * Manage the building of index updates from primary table updates.
 * <p>
 * Internally, parallelizes updates through a thread-pool to a delegate index builder. Underlying
 * {@link IndexBuilder} <b>must be thread safe</b> for each index update.
 */
public class IndexBuildManager implements Stoppable {

  private static final Log LOG = LogFactory.getLog(IndexBuildManager.class);
  private final IndexBuilder delegate;
  private QuickFailingTaskRunner pool;
  private boolean stopped;

  /**
   * Set the number of threads with which we can concurrently build index updates. Unused threads
   * will be released, but setting the number of threads too high could cause frequent swapping and
   * resource contention on the server - <i>tune with care</i>. However, if you are spending a lot
   * of time building index updates, it could be worthwhile to spend the time to tune this parameter
   * as it could lead to dramatic increases in speed.
   */
  public static final String NUM_CONCURRENT_INDEX_BUILDER_THREADS_CONF_KEY = "index.builder.threads.max";
  /** Default to a single thread. This is the safest course of action, but the slowest as well */
  private static final int DEFAULT_CONCURRENT_INDEX_BUILDER_THREADS = 10;
  /**
   * Amount of time to keep idle threads in the pool. After this time (seconds) we expire the
   * threads and will re-create them as needed, up to the configured max
   */
  private static final String INDEX_BUILDER_KEEP_ALIVE_TIME_CONF_KEY =
      "index.builder.threads.keepalivetime";

  /**
   * @param env environment in which <tt>this</tt> is running. Used to setup the
   *          {@link IndexBuilder} and executor
   * @throws IOException if an {@link IndexBuilder} cannot be correctly steup
   */
  public IndexBuildManager(RegionCoprocessorEnvironment env) throws IOException {
    this(getIndexBuilder(env), new QuickFailingTaskRunner(ThreadPoolManager.getExecutor(
      getPoolBuilder(env), env)));
  }

  private static IndexBuilder getIndexBuilder(RegionCoprocessorEnvironment e) throws IOException {
    Configuration conf = e.getConfiguration();
    Class<? extends IndexBuilder> builderClass =
        conf.getClass(Indexer.INDEX_BUILDER_CONF_KEY, null, IndexBuilder.class);
    try {
      IndexBuilder builder = builderClass.newInstance();
      builder.setup(e);
      return builder;
    } catch (InstantiationException e1) {
      throw new IOException("Couldn't instantiate index builder:" + builderClass
          + ", disabling indexing on table " + e.getRegion().getTableDesc().getNameAsString());
    } catch (IllegalAccessException e1) {
      throw new IOException("Couldn't instantiate index builder:" + builderClass
          + ", disabling indexing on table " + e.getRegion().getTableDesc().getNameAsString());
    }
  }

  private static ThreadPoolBuilder getPoolBuilder(RegionCoprocessorEnvironment env) {
    String serverName = env.getRegionServerServices().getServerName().getServerName();
    return new ThreadPoolBuilder(serverName + "-index-builder", env.getConfiguration()).
        setCoreTimeout(INDEX_BUILDER_KEEP_ALIVE_TIME_CONF_KEY).
        setMaxThread(NUM_CONCURRENT_INDEX_BUILDER_THREADS_CONF_KEY,
          DEFAULT_CONCURRENT_INDEX_BUILDER_THREADS);
  }

  public IndexBuildManager(IndexBuilder builder, QuickFailingTaskRunner pool) {
    this.delegate = builder;
    this.pool = pool;
  }


  public Collection<Pair<Mutation, byte[]>> getIndexUpdate(
      MiniBatchOperationInProgress<Mutation> miniBatchOp,
      Collection<? extends Mutation> mutations) throws Throwable {
    // notify the delegate that we have started processing a batch
    this.delegate.batchStarted(miniBatchOp);

    // parallelize each mutation into its own task
    // each task is cancelable via two mechanisms: (1) underlying HRegion is closing (which would
    // fail lookups/scanning) and (2) by stopping this via the #stop method. Interrupts will only be
    // acknowledged on each thread before doing the actual lookup, but after that depends on the
    // underlying builder to look for the closed flag.
    TaskBatch<Collection<Pair<Mutation, byte[]>>> tasks =
        new TaskBatch<Collection<Pair<Mutation, byte[]>>>(mutations.size());
    for (final Mutation m : mutations) {
      tasks.add(new Task<Collection<Pair<Mutation, byte[]>>>() {

        @Override
        public Collection<Pair<Mutation, byte[]>> call() throws IOException {
          return delegate.getIndexUpdate(m);
        }

      });
    }
    List<Collection<Pair<Mutation, byte[]>>> allResults = null;
    try {
      allResults = pool.submitUninterruptible(tasks);
    } catch (CancellationException e) {
      throw e;
    } catch (ExecutionException e) {
      LOG.error("Found a failed index update!");
      throw e.getCause();
    }

    // we can only get here if we get successes from each of the tasks, so each of these must have a
    // correct result
    Collection<Pair<Mutation, byte[]>> results = new ArrayList<Pair<Mutation, byte[]>>();
    for (Collection<Pair<Mutation, byte[]>> result : allResults) {
      assert result != null : "Found an unsuccessful result, but didn't propagate a failure earlier";
      results.addAll(result);
    }

    return results;
  }

  public Collection<Pair<Mutation, byte[]>> getIndexUpdate(Delete delete) throws IOException {
    // all we get is a single update, so it would probably just go slower if we needed to queue it
    // up. It will increase underlying resource contention a little bit, but the mutation case is
    // far more common, so let's not worry about it for now.
    // short circuit so we don't waste time.
    if (!this.delegate.isEnabled(delete)) {
      return null;
    }

    return delegate.getIndexUpdate(delete);

  }

  public Collection<Pair<Mutation, byte[]>> getIndexUpdateForFilteredRows(
      Collection<KeyValue> filtered) throws IOException {
    // this is run async, so we can take our time here
    return delegate.getIndexUpdateForFilteredRows(filtered);
  }

  public void batchCompleted(MiniBatchOperationInProgress<Mutation> miniBatchOp) {
    delegate.batchCompleted(miniBatchOp);
  }

  public void batchStarted(MiniBatchOperationInProgress<Mutation> miniBatchOp)
      throws IOException {
    delegate.batchStarted(miniBatchOp);
  }

  public boolean isEnabled(Mutation m) throws IOException {
    return delegate.isEnabled(m);
  }

  public byte[] getBatchId(Mutation m) {
    return delegate.getBatchId(m);
  }

  @Override
  public void stop(String why) {
    if (stopped) {
      return;
    }
    this.stopped = true;
    this.delegate.stop(why);
    this.pool.stop(why);
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  public IndexBuilder getBuilderForTesting() {
    return this.delegate;
  }
}