/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.phoenix.hbase.index.write.recovery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.phoenix.hbase.index.CapturingAbortable;
import org.apache.phoenix.hbase.index.exception.MultiIndexWriteFailureException;
import org.apache.phoenix.hbase.index.exception.SingleIndexWriteFailureException;
import org.apache.phoenix.hbase.index.parallel.EarlyExitFailure;
import org.apache.phoenix.hbase.index.parallel.Task;
import org.apache.phoenix.hbase.index.parallel.TaskBatch;
import org.apache.phoenix.hbase.index.parallel.TaskRunner;
import org.apache.phoenix.hbase.index.parallel.ThreadPoolBuilder;
import org.apache.phoenix.hbase.index.parallel.ThreadPoolManager;
import org.apache.phoenix.hbase.index.parallel.WaitForCompletionTaskRunner;
import org.apache.phoenix.hbase.index.table.CachingHTableFactory;
import org.apache.phoenix.hbase.index.table.HTableFactory;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.apache.phoenix.hbase.index.write.IndexCommitter;
import org.apache.phoenix.hbase.index.write.IndexWriter;
import org.apache.phoenix.hbase.index.write.IndexWriterUtils;
import org.apache.phoenix.hbase.index.write.ParallelWriterIndexCommitter;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.MetaDataUtil;

import com.google.common.collect.Multimap;

/**
 * Like the {@link ParallelWriterIndexCommitter}, but blocks until all writes have attempted to allow the caller to
 * retrieve the failed and succeeded index updates. Therefore, this class will be a lot slower, in the face of failures,
 * when compared to the {@link ParallelWriterIndexCommitter} (though as fast for writes), so it should be used only when
 * you need to at least attempt all writes and know their result; for instance, this is fine for doing WAL recovery -
 * it's not a performance intensive situation and we want to limit the the edits we need to retry.
 * <p>
 * On failure to {@link #write(Multimap)}, we return a {@link MultiIndexWriteFailureException} that contains the list of
 * {@link HTableInterfaceReference} that didn't complete successfully.
 * <p>
 * Failures to write to the index can happen several different ways:
 * <ol>
 * <li><tt>this</tt> is {@link #stop(String) stopped} or aborted (via the passed {@link Abortable}. This causing any
 * pending tasks to fail whatever they are doing as fast as possible. Any writes that have not begun are not even
 * attempted and marked as failures.</li>
 * <li>A batch write fails. This is the generic HBase write failure - it may occur because the index table is not
 * available, .META. or -ROOT- is unavailable, or any other (of many) possible HBase exceptions.</li>
 * </ol>
 * Regardless of how the write fails, we still wait for all writes to complete before passing the failure back to the
 * client.
 */
public class TrackingParallelWriterIndexCommitter implements IndexCommitter {
    private static final Log LOG = LogFactory.getLog(TrackingParallelWriterIndexCommitter.class);

    public static final String NUM_CONCURRENT_INDEX_WRITER_THREADS_CONF_KEY = "index.trackingwriter.threads.max";
    private static final int DEFAULT_CONCURRENT_INDEX_WRITER_THREADS = 10;
    private static final String INDEX_WRITER_KEEP_ALIVE_TIME_CONF_KEY = "index.trackingwriter.threads.keepalivetime";

    private TaskRunner pool;
    private HTableFactory factory;
    private CapturingAbortable abortable;
    private Stoppable stopped;
    private RegionCoprocessorEnvironment env;

    @Override
    public void setup(IndexWriter parent, RegionCoprocessorEnvironment env, String name) {
        this.env = env;
        Configuration conf = env.getConfiguration();
        setup(IndexWriterUtils.getDefaultDelegateHTableFactory(env),
                ThreadPoolManager.getExecutor(
                        new ThreadPoolBuilder(name, conf).setMaxThread(NUM_CONCURRENT_INDEX_WRITER_THREADS_CONF_KEY,
                                DEFAULT_CONCURRENT_INDEX_WRITER_THREADS).setCoreTimeout(
                                INDEX_WRITER_KEEP_ALIVE_TIME_CONF_KEY), env), env.getRegionServerServices(), parent,
                CachingHTableFactory.getCacheSize(conf));
    }

    /**
     * Setup <tt>this</tt>.
     * <p>
     * Exposed for TESTING
     */
    void setup(HTableFactory factory, ExecutorService pool, Abortable abortable, Stoppable stop, int cacheSize) {
        this.pool = new WaitForCompletionTaskRunner(pool);
        this.factory = new CachingHTableFactory(factory, cacheSize);
        this.abortable = new CapturingAbortable(abortable);
        this.stopped = stop;
    }

    @Override
    public void write(Multimap<HTableInterfaceReference, Mutation> toWrite) throws MultiIndexWriteFailureException {
        Set<Entry<HTableInterfaceReference, Collection<Mutation>>> entries = toWrite.asMap().entrySet();
        TaskBatch<Boolean> tasks = new TaskBatch<Boolean>(entries.size());
        List<HTableInterfaceReference> tables = new ArrayList<HTableInterfaceReference>(entries.size());
        for (Entry<HTableInterfaceReference, Collection<Mutation>> entry : entries) {
            // get the mutations for each table. We leak the implementation here a little bit to save
            // doing a complete copy over of all the index update for each table.
            final List<Mutation> mutations = (List<Mutation>)entry.getValue();
            // track each reference so we can get at it easily later, when determing failures
            final HTableInterfaceReference tableReference = entry.getKey();
            final RegionCoprocessorEnvironment env = this.env;
            tables.add(tableReference);

            /*
             * Write a batch of index updates to an index table. This operation stops (is cancelable) via two
             * mechanisms: (1) setting aborted or stopped on the IndexWriter or, (2) interrupting the running thread.
             * The former will only work if we are not in the midst of writing the current batch to the table, though we
             * do check these status variables before starting and before writing the batch. The latter usage,
             * interrupting the thread, will work in the previous situations as was at some points while writing the
             * batch, depending on the underlying writer implementation (HTableInterface#batch is blocking, but doesn't
             * elaborate when is supports an interrupt).
             */
            tasks.add(new Task<Boolean>() {

                /**
                 * Do the actual write to the primary table. We don't need to worry about closing the table because that
                 * is handled the {@link CachingHTableFactory}.
                 */
                @SuppressWarnings("deprecation")
                @Override
                public Boolean call() throws Exception {
                    try {
                        // this may have been queued, but there was an abort/stop so we try to early exit
                        throwFailureIfDone();

                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Writing index update:" + mutations + " to table: " + tableReference);
                        }

                        try {
                            // TODO: Once HBASE-11766 is fixed, reexamine whether this is necessary.
                            // Also, checking the prefix of the table name to determine if this is a local
                            // index is pretty hacky. If we're going to keep this, we should revisit that
                            // as well.
                            if (tableReference.getTableName().startsWith(MetaDataUtil.LOCAL_INDEX_TABLE_PREFIX)) {
                                HRegion indexRegion = IndexUtil.getIndexRegion(env);
                                if (indexRegion != null) {
                                    throwFailureIfDone();
                                    indexRegion.batchMutate(mutations.toArray(new Mutation[mutations.size()]));
                                    return Boolean.TRUE;
                                }
                            }
                        } catch (IOException ignord) {
                            // when it's failed we fall back to the standard & slow way
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("indexRegion.batchMutate failed and fall back to HTable.batch(). Got error="
                                        + ignord);
                            }
                        }

                        HTableInterface table = factory.getTable(tableReference.get());
                        throwFailureIfDone();
                        table.batch(mutations);
                    } catch (InterruptedException e) {
                        // reset the interrupt status on the thread
                        Thread.currentThread().interrupt();
                        throw e;
                    } catch (Exception e) {
                        throw e;
                    }
                    return Boolean.TRUE;
                }

                private void throwFailureIfDone() throws SingleIndexWriteFailureException {
                    if (stopped.isStopped() || abortable.isAborted() || Thread.currentThread().isInterrupted()) { throw new SingleIndexWriteFailureException(
                            "Pool closed, not attempting to write to the index!", null); }

                }
            });
        }

        List<Boolean> results = null;
        try {
            LOG.debug("Waiting on index update tasks to complete...");
            results = this.pool.submitUninterruptible(tasks);
        } catch (ExecutionException e) {
            throw new RuntimeException("Should not fail on the results while using a WaitForCompletionTaskRunner", e);
        } catch (EarlyExitFailure e) {
            throw new RuntimeException("Stopped while waiting for batch, quiting!", e);
        }

        // track the failures. We only ever access this on return from our calls, so no extra
        // synchronization is needed. We could update all the failures as we find them, but that add a
        // lot of locking overhead, and just doing the copy later is about as efficient.
        List<HTableInterfaceReference> failures = new ArrayList<HTableInterfaceReference>();
        int index = 0;
        for (Boolean result : results) {
            // there was a failure
            if (result == null) {
                // we know which table failed by the index of the result
                failures.add(tables.get(index));
            }
            index++;
        }

        // if any of the tasks failed, then we need to propagate the failure
        if (failures.size() > 0) {
            // make the list unmodifiable to avoid any more synchronization concerns
            throw new MultiIndexWriteFailureException(Collections.unmodifiableList(failures));
        }
        return;
    }

    @Override
    public void stop(String why) {
        LOG.info("Shutting down " + this.getClass().getSimpleName());
        this.pool.stop(why);
        this.factory.shutdown();
    }

    @Override
    public boolean isStopped() {
        return this.stopped.isStopped();
    }
}