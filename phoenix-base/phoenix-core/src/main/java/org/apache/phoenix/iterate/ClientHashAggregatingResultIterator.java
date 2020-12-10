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
package org.apache.phoenix.iterate;

import static org.apache.phoenix.query.QueryConstants.AGG_TIMESTAMP;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN_FAMILY;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.ExplainPlanAttributes
    .ExplainPlanAttributesBuilder;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.Aggregators;
import org.apache.phoenix.memory.MemoryManager.MemoryChunk;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.apache.phoenix.util.SizedUtil;
import org.apache.phoenix.util.TupleUtil;

/**
 * 
 * This class implements client-side hash aggregation in memory.
 * Issue https://issues.apache.org/jira/browse/PHOENIX-4751.
 * 
 */
public class ClientHashAggregatingResultIterator
    implements AggregatingResultIterator {

    private static final int HASH_AGG_INIT_SIZE = 64*1024;
    private static final int CLIENT_HASH_AGG_MEMORY_CHUNK_SIZE = 64*1024;
    private static final byte[] UNITIALIZED_KEY_BUFFER = new byte[0];
    private final ResultIterator resultIterator;
    private final Aggregators aggregators;
    private final List<Expression> groupByExpressions;
    private final OrderBy orderBy;
    private final MemoryChunk memoryChunk;
    private HashMap<ImmutableBytesWritable, Aggregator[]> hash;
    private List<ImmutableBytesWritable> keyList;
    private Iterator<ImmutableBytesWritable> keyIterator;

    public ClientHashAggregatingResultIterator(StatementContext context, ResultIterator resultIterator,
                                               Aggregators aggregators, List<Expression> groupByExpressions, OrderBy orderBy) {

        Objects.requireNonNull(resultIterator);
        Objects.requireNonNull(aggregators);
        Objects.requireNonNull(groupByExpressions);
        this.resultIterator = resultIterator;
        this.aggregators = aggregators;
        this.groupByExpressions = groupByExpressions;
        this.orderBy = orderBy;
        memoryChunk = context.getConnection().getQueryServices().getMemoryManager().allocate(CLIENT_HASH_AGG_MEMORY_CHUNK_SIZE);
    }

    @Override
    public Tuple next() throws SQLException {
        if (keyIterator == null) {
            hash = populateHash();
            /********
             *
             * Perform a post-aggregation sort only when required. There are 3 possible scenarios:
             * (1) The query DOES NOT have an ORDER BY -- in this case, we DO NOT perform a sort, and the results will be in random order.
             * (2) The query DOES have an ORDER BY, the ORDER BY keys match the GROUP BY keys, and all the ORDER BY keys are ASCENDING
             *     -- in this case, we DO perform a sort. THE ORDER BY has been optimized away, because the non-hash client aggregation
             *        generates results in ascending order of the GROUP BY keys.
             * (3) The query DOES have an ORDER BY, but the ORDER BY keys do not match the GROUP BY keys, or at least one ORDER BY key is DESCENDING
             *     -- in this case, we DO NOT perform a sort, because the ORDER BY has not been optimized away and will be performed later by the
             *        client aggregation code.
             *
             * Finally, we also handle optimization of reverse sort here. This is currently defensive, because reverse sort is not optimized away.
             *
             ********/
            if (orderBy == OrderBy.FWD_ROW_KEY_ORDER_BY || orderBy == OrderBy.REV_ROW_KEY_ORDER_BY) {
                keyList = sortKeys();
                keyIterator = keyList.iterator();
            } else {
                keyIterator = hash.keySet().iterator();
            }
        }

        if (!keyIterator.hasNext()) {
            return null;
        }

        ImmutableBytesWritable key = keyIterator.next();
        Aggregator[] rowAggregators = hash.get(key);
        byte[] value = aggregators.toBytes(rowAggregators);
        Tuple tuple = wrapKeyValueAsResult(PhoenixKeyValueUtil.newKeyValue(key, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, AGG_TIMESTAMP, value, 0, value.length));
        return tuple;
    }

    @Override
    public void close() throws SQLException {
        keyIterator = null;
        keyList = null;
        hash = null;
        try {
            memoryChunk.close();
        } finally {
            resultIterator.close();
        }
    }

    @Override
    public Aggregator[] aggregate(Tuple result) {
        Aggregator[] rowAggregators = aggregators.getAggregators();
        aggregators.reset(rowAggregators);
        aggregators.aggregate(rowAggregators, result);
        return rowAggregators;
    }

    @Override
    public void explain(List<String> planSteps) {
        resultIterator.explain(planSteps);
    }

    @Override
    public void explain(List<String> planSteps,
            ExplainPlanAttributesBuilder explainPlanAttributesBuilder) {
        resultIterator.explain(planSteps, explainPlanAttributesBuilder);
    }

    @Override
        public String toString() {
        return "ClientHashAggregatingResultIterator [resultIterator="
            + resultIterator + ", aggregators=" + aggregators + ", groupByExpressions="
            + groupByExpressions + "]";
    }

    // Copied from ClientGroupedAggregatingResultIterator
    protected ImmutableBytesWritable getGroupingKey(Tuple tuple, ImmutableBytesWritable ptr) throws SQLException {
        try {
            ImmutableBytesWritable key = TupleUtil.getConcatenatedValue(tuple, groupByExpressions);
            ptr.set(key.get(), key.getOffset(), key.getLength());
            return ptr;
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    // Copied from ClientGroupedAggregatingResultIterator
    protected Tuple wrapKeyValueAsResult(Cell keyValue) {
        return new MultiKeyValueTuple(Collections.<Cell> singletonList(keyValue));
    }

    private HashMap<ImmutableBytesWritable, Aggregator[]> populateHash() throws SQLException {

        hash = new HashMap<ImmutableBytesWritable, Aggregator[]>(HASH_AGG_INIT_SIZE, 0.75f);
        final int aggSize = aggregators.getEstimatedByteSize();
        long keySize = 0;

        for (Tuple result = resultIterator.next(); result != null; result = resultIterator.next()) {
            ImmutableBytesWritable key = new ImmutableBytesWritable(UNITIALIZED_KEY_BUFFER);
            key = getGroupingKey(result, key);
            Aggregator[] rowAggregators = hash.get(key);
            if (rowAggregators == null) {
                keySize += key.getSize();
                long hashSize = SizedUtil.sizeOfMap(hash.size() + 1, SizedUtil.IMMUTABLE_BYTES_WRITABLE_SIZE, aggSize) + keySize;
                if (hashSize > memoryChunk.getSize() + CLIENT_HASH_AGG_MEMORY_CHUNK_SIZE) {
                    // This will throw InsufficientMemoryException if necessary
                    memoryChunk.resize(hashSize + CLIENT_HASH_AGG_MEMORY_CHUNK_SIZE);
                }

                rowAggregators = aggregators.newAggregators();
                hash.put(key, rowAggregators);
            }

            aggregators.aggregate(rowAggregators, result);
        }

        return hash;
    }

    private List<ImmutableBytesWritable> sortKeys() {
        // This will throw InsufficientMemoryException if necessary
        memoryChunk.resize(memoryChunk.getSize() + SizedUtil.sizeOfArrayList(hash.size()));

        keyList = new ArrayList<ImmutableBytesWritable>(hash.size());
        keyList.addAll(hash.keySet());
        Comparator<ImmutableBytesWritable> comp = new ImmutableBytesWritable.Comparator();
        if (orderBy == OrderBy.REV_ROW_KEY_ORDER_BY) {
            comp = Collections.reverseOrder(comp);
        }
        Collections.sort(keyList, comp);
        return keyList;
    }
}
