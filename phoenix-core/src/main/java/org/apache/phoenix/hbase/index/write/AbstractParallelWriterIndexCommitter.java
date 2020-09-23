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
package org.apache.phoenix.hbase.index.write;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.hbase.index.exception.SingleIndexWriteFailureException;
import org.apache.phoenix.hbase.index.parallel.QuickFailingTaskRunner;
import org.apache.phoenix.hbase.index.parallel.Task;
import org.apache.phoenix.hbase.index.parallel.TaskBatch;
import org.apache.phoenix.hbase.index.parallel.ThreadPoolBuilder;
import org.apache.phoenix.hbase.index.parallel.ThreadPoolManager;
import org.apache.phoenix.hbase.index.table.HTableFactory;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.index.PhoenixIndexFailurePolicy;
import org.apache.phoenix.util.IndexUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.Multimap;

/**
 * Abstract class to Write index updates to the index tables in parallel.
 */
public abstract class AbstractParallelWriterIndexCommitter implements IndexCommitter {

    public static final String NUM_CONCURRENT_INDEX_WRITER_THREADS_CONF_KEY = "index.writer.threads.max";
    private static final int DEFAULT_CONCURRENT_INDEX_WRITER_THREADS = 10;
    public static final String INDEX_WRITER_KEEP_ALIVE_TIME_CONF_KEY = "index.writer.threads.keepalivetime";
    private static final Logger LOG = LoggerFactory.getLogger(IndexWriter.class);

    protected HTableFactory retryingFactory;
    protected HTableFactory noRetriesFactory;
    protected Stoppable stopped;
    protected QuickFailingTaskRunner pool;
    protected KeyValueBuilder kvBuilder;
    protected RegionCoprocessorEnvironment env;
    protected TaskBatch<Void> tasks;
    protected boolean disableIndexOnFailure = false;


    public AbstractParallelWriterIndexCommitter() {}

    // For testing
    public AbstractParallelWriterIndexCommitter(String hbaseVersion) {
        kvBuilder = KeyValueBuilder.get(hbaseVersion);
    }

    @Override
    public void setup(IndexWriter parent, RegionCoprocessorEnvironment env, String name, boolean disableIndexOnFailure) {
        this.env = env;
        this.disableIndexOnFailure = disableIndexOnFailure;
        Configuration conf = env.getConfiguration();
        setup(IndexWriterUtils.getDefaultDelegateHTableFactory(env),
                ThreadPoolManager.getExecutor(
                        new ThreadPoolBuilder(name, conf).setMaxThread(NUM_CONCURRENT_INDEX_WRITER_THREADS_CONF_KEY,
                                DEFAULT_CONCURRENT_INDEX_WRITER_THREADS).setCoreTimeout(
                                INDEX_WRITER_KEEP_ALIVE_TIME_CONF_KEY), env), parent, env);
        this.kvBuilder = KeyValueBuilder.get(env.getHBaseVersion());
    }

    /**
     * Setup <tt>this</tt>.
     * <p>
     * Exposed for TESTING
     */
    public void setup(HTableFactory factory, ExecutorService pool,Stoppable stop, RegionCoprocessorEnvironment env) {
        this.retryingFactory = factory;
        this.noRetriesFactory = IndexWriterUtils.getNoRetriesHTableFactory(env);
        this.pool = new QuickFailingTaskRunner(pool);
        this.stopped = stop;
        this.env = env;
    }

    @Override
    public void write(Multimap<HTableInterfaceReference, Mutation> toWrite, final boolean allowLocalUpdates, final int clientVersion) throws SingleIndexWriteFailureException {
        /*
         * This bit here is a little odd, so let's explain what's going on. Basically, we want to do the writes in
         * parallel to each index table, so each table gets its own task and is submitted to the pool. Where it gets
         * tricky is that we want to block the calling thread until one of two things happens: (1) all index tables get
         * successfully updated, or (2) any one of the index table writes fail; in either case, we should return as
         * quickly as possible. We get a little more complicated in that if we do get a single failure, but any of the
         * index writes hasn't been started yet (its been queued up, but not submitted to a thread) we want to that task
         * to fail immediately as we know that write is a waste and will need to be replayed anyways.
         */

        Set<Entry<HTableInterfaceReference, Collection<Mutation>>> entries = toWrite.asMap().entrySet();
        tasks = new TaskBatch<Void>(entries.size());
        for (Entry<HTableInterfaceReference, Collection<Mutation>> entry : entries) {
            // get the mutations for each table. We leak the implementation here a little bit to save
            // doing a complete copy over of all the index update for each table.
            final List<Mutation> mutations = kvBuilder.cloneIfNecessary((List<Mutation>)entry.getValue());
            final HTableInterfaceReference tableReference = entry.getKey();
			if (env != null
					&& !allowLocalUpdates
					&& tableReference.getTableName().equals(
							env.getRegion().getTableDescriptor().getTableName().getNameAsString())) {
				continue;
			}
            /*
             * Write a batch of index updates to an index table. This operation stops (is cancelable) via two
             * mechanisms: (1) setting aborted or stopped on the IndexWriter or, (2) interrupting the running thread.
             * The former will only work if we are not in the midst of writing the current batch to the table, though we
             * do check these status variables before starting and before writing the batch. The latter usage,
             * interrupting the thread, will work in the previous situations as was at some points while writing the
             * batch, depending on the underlying writer implementation (HTableInterface#batch is blocking, but doesn't
             * elaborate when is supports an interrupt).
             */
            tasks.add(new Task<Void>() {

                /**
                 * Do the actual write to the primary table.
                 * 
                 * @return
                 */
                @SuppressWarnings("deprecation")
                @Override
                public Void call() throws Exception {
                    // this may have been queued, so another task infront of us may have failed, so we should
                    // early exit, if that's the case
                    throwFailureIfDone();

                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Writing index update:" + mutations + " to table: " + tableReference);
                    }
                    try {
                        if (allowLocalUpdates
                                && env != null
                                && tableReference.getTableName().equals(
                                    env.getRegion().getTableDescriptor().getTableName().getNameAsString())) {
                            try {
                                throwFailureIfDone();
                                IndexUtil.writeLocalUpdates(env.getRegion(), mutations, true);
                                return null;
                            } catch (IOException ignored) {
                                // when it's failed we fall back to the standard & slow way
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("indexRegion.batchMutate failed and fall back to HTable.batch(). Got error="
                                            + ignored);
                                }
                            }
                        }
                        // if the client can retry index writes, then we don't need to retry here
                        HTableFactory factory;
                        if (disableIndexOnFailure) {
                            factory = clientVersion < MetaDataProtocol.MIN_CLIENT_RETRY_INDEX_WRITES ? retryingFactory : noRetriesFactory;
                        }
                        else {
                            factory = retryingFactory;
                        }
                        try (Table table = factory.getTable(tableReference.get())) {
                            throwFailureIfDone();
                            table.batch(mutations, null);
                        }
                    } catch (SingleIndexWriteFailureException e) {
                        throw e;
                    } catch (IOException e) {
                        throw new SingleIndexWriteFailureException(tableReference.toString(), mutations, e, PhoenixIndexFailurePolicy.getDisableIndexOnFailure(env));
                    } catch (InterruptedException e) {
                        // reset the interrupt status on the thread
                        Thread.currentThread().interrupt();
                        throw new SingleIndexWriteFailureException(tableReference.toString(), mutations, e, PhoenixIndexFailurePolicy.getDisableIndexOnFailure(env));
                    }
                    return null;
                }

                private void throwFailureIfDone() throws SingleIndexWriteFailureException {
                    if (this.isBatchFailed() || Thread.currentThread().isInterrupted()) { throw new SingleIndexWriteFailureException(
                            "Pool closed, not attempting to write to the index!", null); }

                }
            });
        }
    }

    protected void propagateFailure(Throwable throwable) throws SingleIndexWriteFailureException {
        try {
            throw throwable;
        } catch (SingleIndexWriteFailureException e1) {
            throw e1;
        } catch (Throwable e1) {
            throw new SingleIndexWriteFailureException("Got an abort notification while writing to the index!", e1);
        }

    }

    /**
     * {@inheritDoc}
     * <p>
     * This method should only be called <b>once</b>. Stopped state ({@link #isStopped()}) is managed by the external
     * {@link Stoppable}. This call does not delegate the stop down to the {@link Stoppable} passed in the constructor.
     * 
     * @param why
     *            the reason for stopping
     */
    @Override
    public void stop(String why) {
        LOG.info("Shutting down " + this.getClass().getSimpleName() + " because " + why);
        this.pool.stop(why);
        this.retryingFactory.shutdown();
        this.noRetriesFactory.shutdown();
    }

    @Override
    public boolean isStopped() {
        return this.stopped.isStopped();
    }
}
