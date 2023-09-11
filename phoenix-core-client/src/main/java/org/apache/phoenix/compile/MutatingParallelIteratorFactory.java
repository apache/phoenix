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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.ExplainPlanAttributes
    .ExplainPlanAttributesBuilder;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.iterate.ParallelIteratorFactory;
import org.apache.phoenix.iterate.PeekingResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class used to instantiate an iterator to handle mutations made during a parallel scan.
 */
public abstract class MutatingParallelIteratorFactory implements ParallelIteratorFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(
            MutatingParallelIteratorFactory.class);
    protected final PhoenixConnection connection;

    protected MutatingParallelIteratorFactory(PhoenixConnection connection) {
        this.connection = connection;
    }
    
    /**
     * Method that does the actual mutation work
     */
    abstract protected MutationState mutate(StatementContext parentContext, ResultIterator iterator,
            PhoenixConnection connection) throws SQLException;
    
    @Override
    public PeekingResultIterator newIterator(final StatementContext parentContext,
            ResultIterator iterator, Scan scan, String tableName,
            QueryPlan plan) throws SQLException {

        final PhoenixConnection clonedConnection = new PhoenixConnection(this.connection);
        connection.addChildConnection(clonedConnection);
        try {
            MutationState state = mutate(parentContext, iterator, clonedConnection);

            final long totalRowCount = state.getUpdateCount();
            final boolean autoFlush = connection.getAutoCommit() ||
                    plan.getTableRef().getTable().isTransactional();
            if (autoFlush) {
                clonedConnection.getMutationState().join(state);
                state = clonedConnection.getMutationState();
            }
            final MutationState finalState = state;

            byte[] value = PLong.INSTANCE.toBytes(totalRowCount);
            Cell keyValue = PhoenixKeyValueUtil.newKeyValue(UNGROUPED_AGG_ROW_KEY,
                    SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, AGG_TIMESTAMP, value, 0, value.length);
            final Tuple tuple = new SingleKeyValueTuple(keyValue);
            return new PeekingResultIterator() {
                private boolean done = false;

                @Override
                public Tuple next() {
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
                public void explain(List<String> planSteps,
                    ExplainPlanAttributesBuilder explainPlanAttributesBuilder) {
                }

                @Override
                public void close() throws SQLException {
                    try {
                        /*
                         * Join the child mutation states in close, since this is called in a single
                         * threaded manner after the parallel results have been processed.
                         * If auto-commit is on for the cloned child connection, then the finalState
                         * here is an empty mutation state (with no mutations). However, it still
                         * has the metrics for mutation work done by the mutating-iterator.
                         * Joining the mutation state makes sure those metrics are passed over
                         * to the parent connection.
                         */
                        MutatingParallelIteratorFactory.this.connection.getMutationState()
                                .join(finalState);
                    } finally {
                        //Removing to be closed connection from the parent connection queue.
                        connection.removeChildConnection(clonedConnection);
                        clonedConnection.close();
                    }
                }

                @Override
                public Tuple peek() {
                    return done ? null : tuple;
                }
            };
        } catch (Throwable ex) {
            // Catch just to make sure we close the cloned connection and then rethrow
            try {
                //Removing to be closed connection from the parent connection queue.
                connection.removeChildConnection(clonedConnection);
                // closeQuietly only handles IOException
                clonedConnection.close();
            } catch (SQLException sqlEx) {
                LOGGER.error("Closing cloned Phoenix connection inside iterator, failed with: ",
                        sqlEx);
            }
            throw ex;
        }
    }
}
