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
package org.apache.phoenix.compile;

import static org.apache.phoenix.query.QueryConstants.AGG_TIMESTAMP;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN_FAMILY;
import static org.apache.phoenix.query.QueryConstants.UNGROUPED_AGG_ROW_KEY;

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.iterate.ParallelIteratorFactory;
import org.apache.phoenix.iterate.PeekingResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.KeyValueUtil;

/**
 * Factory class used to instantiate an iterator to handle mutations made during a parallel scan.
 */
public abstract class MutatingParallelIteratorFactory implements ParallelIteratorFactory {
    protected final PhoenixConnection connection;

    protected MutatingParallelIteratorFactory(PhoenixConnection connection) {
        this.connection = connection;
    }
    
    /**
     * Method that does the actual mutation work
     */
    abstract protected MutationState mutate(StatementContext parentContext, ResultIterator iterator, PhoenixConnection connection) throws SQLException;
    
    @Override
    public PeekingResultIterator newIterator(final StatementContext parentContext, ResultIterator iterator, Scan scan, String tableName) throws SQLException {
        final PhoenixConnection clonedConnection = new PhoenixConnection(this.connection);
        
        MutationState state = mutate(parentContext, iterator, clonedConnection);
        
        long totalRowCount = state.getUpdateCount();
        if (clonedConnection.getAutoCommit()) {
            clonedConnection.getMutationState().join(state);
            clonedConnection.commit();
            ConnectionQueryServices services = clonedConnection.getQueryServices();
            int maxSize = services.getProps().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB, QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
            /*
             * Everything that was mutated as part of the clonedConnection has been committed. However, we want to
             * report the mutation work done using this clonedConnection as part of the overall mutation work of the
             * parent connection. So we need to set those metrics in the empty mutation state so that they could be
             * combined with the parent connection's mutation metrics (as part of combining mutation state) in the
             * close() method of the iterator being returned. Don't combine the read metrics in parent context yet
             * though because they are possibly being concurrently modified by other threads at this stage. Instead we
             * will get hold of the read metrics when all the mutating iterators are done.
             */
            state = MutationState.emptyMutationState(maxSize, clonedConnection);
            state.getMutationMetricQueue().combineMetricQueues(clonedConnection.getMutationState().getMutationMetricQueue());
        }
        final MutationState finalState = state;
        
        byte[] value = PLong.INSTANCE.toBytes(totalRowCount);
        KeyValue keyValue = KeyValueUtil.newKeyValue(UNGROUPED_AGG_ROW_KEY, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, AGG_TIMESTAMP, value, 0, value.length);
        final Tuple tuple = new SingleKeyValueTuple(keyValue);
        return new PeekingResultIterator() {
            private boolean done = false;
            
            @Override
            public Tuple next() throws SQLException {
                if (done) {
                    return null;
                }
                done = true;
                return tuple;
            }

            @Override
            public void explain(List<String> planSteps) {
            }

            @Override
            public void close() throws SQLException {
                try {
                    /* 
                     * Join the child mutation states in close, since this is called in a single threaded manner
                     * after the parallel results have been processed. 
                     * If auto-commit is on for the cloned child connection, then the finalState here is an empty mutation 
                     * state (with no mutations). However, it still has the metrics for mutation work done by the 
                     * mutating-iterator. Joining the mutation state makes sure those metrics are passed over
                     * to the parent connection.
                     */ 
                    MutatingParallelIteratorFactory.this.connection.getMutationState().join(finalState);
                } finally {
                    clonedConnection.close();
                }
            }

            @Override
            public Tuple peek() throws SQLException {
                return done ? null : tuple;
            }
        };
    }
}
