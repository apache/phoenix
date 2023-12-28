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
package org.apache.phoenix.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.apache.phoenix.coprocessor.DelegateRegionCoprocessorEnvironment;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol;
import org.apache.phoenix.execute.PhoenixTxIndexMutationGenerator;
import org.apache.phoenix.hbase.index.write.IndexWriter;
import org.apache.phoenix.hbase.index.write.LeaveIndexActiveFailurePolicy;
import org.apache.phoenix.hbase.index.write.ParallelWriterIndexCommitter;
import org.apache.phoenix.trace.TracingUtils;
import org.apache.phoenix.trace.util.NullSpan;
import org.apache.phoenix.transaction.PhoenixTransactionContext;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.ServerUtil.ConnectionType;
import org.apache.phoenix.util.TransactionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Do all the work of managing local index updates for a transactional table from a single coprocessor. Since the transaction
 * manager essentially time orders writes through conflict detection, the logic to maintain a secondary index is quite a
 * bit simpler than the non transactional case. For example, there's no need to muck with the WAL, as failure scenarios
 * are handled by aborting the transaction.
 */
public class PhoenixTransactionalIndexer implements RegionObserver, RegionCoprocessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixTransactionalIndexer.class);

    // Hack to get around not being able to save any state between
    // coprocessor calls. TODO: remove after HBASE-18127 when available
    private static class BatchMutateContext {
        public Collection<Pair<Mutation, byte[]>> indexUpdates = Collections.emptyList();
        public final int clientVersion;

        public BatchMutateContext(int clientVersion) {
            this.clientVersion = clientVersion;
        }
    }
    
    private ThreadLocal<BatchMutateContext> batchMutateContext =
            new ThreadLocal<BatchMutateContext>();
    
    private PhoenixIndexCodec codec;
    private IndexWriter writer;
    private boolean stopped;
    
    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }
    
    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        final RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment)e;
        String serverName = env.getServerName().getServerName();
        codec = new PhoenixIndexCodec(env.getConfiguration(), env.getRegionInfo().getTable().getName());
        DelegateRegionCoprocessorEnvironment indexWriterEnv = new DelegateRegionCoprocessorEnvironment(env, ConnectionType.INDEX_WRITER_CONNECTION);
        // setup the actual index writer
        // For transactional tables, we keep the index active upon a write failure
        // since we have the all versus none behavior for transactions. Also, we
        // fail on any write exception since this will end up failing the transaction.
        this.writer = new IndexWriter(IndexWriter.getCommitter(indexWriterEnv, ParallelWriterIndexCommitter.class),
                new LeaveIndexActiveFailurePolicy(), indexWriterEnv, serverName + "-tx-index-writer");
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        if (this.stopped) { return; }
        this.stopped = true;
        String msg = "TxIndexer is being stopped";
        this.writer.stop(msg);
    }

    private static Iterator<Mutation> getMutationIterator(final MiniBatchOperationInProgress<Mutation> miniBatchOp) {
        return new Iterator<Mutation>() {
            private int i = 0;
            
            @Override
            public boolean hasNext() {
                return i < miniBatchOp.size();
            }

            @Override
            public Mutation next() {
                return miniBatchOp.getOperation(i++);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
            
        };
    }
    
    @Override
    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
            MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {

        Mutation m = miniBatchOp.getOperation(0);
        if (!codec.isEnabled(m)) {
            return;
        }

        PhoenixIndexMetaData indexMetaData = new PhoenixIndexMetaDataBuilder(c.getEnvironment()).getIndexMetaData(miniBatchOp);
        if (    indexMetaData.getClientVersion() >= MetaDataProtocol.MIN_TX_CLIENT_SIDE_MAINTENANCE
            && !indexMetaData.hasLocalIndexes()) { // Still generate index updates server side for local indexes
            return;
        }
        BatchMutateContext context = new BatchMutateContext(indexMetaData.getClientVersion());
        setBatchMutateContext(c, context);
        
        Collection<Pair<Mutation, byte[]>> indexUpdates = null;
        // get the current span, or just use a null-span to avoid a bunch of if statements
        try (TraceScope scope = Trace.startSpan("Starting to build index updates")) {
            Span current = scope.getSpan();
            if (current == null) {
                current = NullSpan.INSTANCE;
            }

            RegionCoprocessorEnvironment env = c.getEnvironment();
            PhoenixTransactionContext txnContext = indexMetaData.getTransactionContext();
            if (txnContext == null) {
                throw new NullPointerException("Expected to find transaction in metadata for " + env.getRegionInfo().getTable().getNameAsString());
            }
            PhoenixTxIndexMutationGenerator generator = new PhoenixTxIndexMutationGenerator(env.getConfiguration(), indexMetaData,
                    env.getRegionInfo().getTable().getName(), 
                    env.getRegionInfo().getStartKey(), 
                    env.getRegionInfo().getEndKey());
            try (Table htable = env.getConnection().getTable(env.getRegionInfo().getTable())) {
                // get the index updates for all elements in this batch
                indexUpdates = generator.getIndexUpdates(htable, getMutationIterator(miniBatchOp));
            }
            byte[] tableName = c.getEnvironment().getRegionInfo().getTable().getName();
            Iterator<Pair<Mutation, byte[]>> indexUpdatesItr = indexUpdates.iterator();
            List<Mutation> localUpdates = new ArrayList<Mutation>(indexUpdates.size());
            while(indexUpdatesItr.hasNext()) {
                Pair<Mutation, byte[]> next = indexUpdatesItr.next();
                if (Bytes.compareTo(next.getSecond(), tableName) == 0) {
                    // These mutations will not go through the preDelete hooks, so we
                    // must manually convert them here.
                    Mutation mutation = TransactionUtil.convertIfDelete(next.getFirst());
                    localUpdates.add(mutation);
                    indexUpdatesItr.remove();
                }
            }
            if (!localUpdates.isEmpty()) {
                miniBatchOp.addOperationsFromCP(0,
                    localUpdates.toArray(new Mutation[localUpdates.size()]));
            }
            if (!indexUpdates.isEmpty()) {
                context.indexUpdates = indexUpdates;
            }

            current.addTimelineAnnotation("Built index updates, doing preStep");
            TracingUtils.addAnnotation(current, "index update count", context.indexUpdates.size());
        } catch (Throwable t) {
            String msg = "Failed to update index with entries:" + indexUpdates;
            LOGGER.error(msg, t);
            ClientUtil.throwIOException(msg, t);
        }
    }

    @Override
    public void postBatchMutateIndispensably(ObserverContext<RegionCoprocessorEnvironment> c,
        MiniBatchOperationInProgress<Mutation> miniBatchOp, final boolean success) throws IOException {
        BatchMutateContext context = getBatchMutateContext(c);
        if (context == null || context.indexUpdates == null) {
            return;
        }
        // get the current span, or just use a null-span to avoid a bunch of if statements
        try (TraceScope scope = Trace.startSpan("Starting to write index updates")) {
            Span current = scope.getSpan();
            if (current == null) {
                current = NullSpan.INSTANCE;
            }

            if (success) { // if miniBatchOp was successfully written, write index updates
                if (!context.indexUpdates.isEmpty()) {
                    this.writer.write(context.indexUpdates, false, context.clientVersion);
                }
                current.addTimelineAnnotation("Wrote index updates");
            }
        } catch (Throwable t) {
            String msg = "Failed to write index updates:" + context.indexUpdates;
            LOGGER.error(msg, t);
            ClientUtil.throwIOException(msg, t);
         } finally {
             removeBatchMutateContext(c);
         }
    }

    private void setBatchMutateContext(ObserverContext<RegionCoprocessorEnvironment> c, BatchMutateContext context) {
        this.batchMutateContext.set(context);
    }
    
    private BatchMutateContext getBatchMutateContext(ObserverContext<RegionCoprocessorEnvironment> c) {
        return this.batchMutateContext.get();
    }
    
    private void removeBatchMutateContext(ObserverContext<RegionCoprocessorEnvironment> c) {
        this.batchMutateContext.remove();
    }
}
