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

import java.sql.SQLException;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.Aggregators;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.KeyValueUtil;

/**
 * 
 * This class implements client-side hash aggregation in memory.
 * Issue https://issues.apache.org/jira/browse/PHOENIX-4751.
 * 
 */
public class ClientHashAggregatingResultIterator
    implements AggregatingResultIterator {

    private static final int HASH_AGG_INIT_SIZE = 64*1024;
    private static final byte[] UNITIALIZED_KEY_BUFFER = new byte[0];
    private final ResultIterator resultIterator;
    private final Aggregators aggregators;
    private final List<Expression> groupByExpressions;
    private final HashMap<ImmutableBytesWritable, Aggregator[]> hash;
    private Iterator<Map.Entry<ImmutableBytesWritable, Aggregator[]>> hashIterator;

    public ClientHashAggregatingResultIterator(ResultIterator resultIterator, Aggregators aggregators, List<Expression> groupByExpressions) {
        if (resultIterator == null) throw new NullPointerException();
        if (aggregators == null) throw new NullPointerException();
        if (groupByExpressions == null) throw new NullPointerException();
        this.resultIterator = resultIterator;
        this.aggregators = aggregators;
        this.groupByExpressions = groupByExpressions;
        hash = new HashMap<ImmutableBytesWritable, Aggregator[]>(HASH_AGG_INIT_SIZE, 0.75f);
    }

    @Override
    public Tuple next() throws SQLException {
        if (hashIterator == null) {
            populateHash();
            hashIterator = hash.entrySet().iterator();
        }

        if (!hashIterator.hasNext()) {
            return null;
        }

        Map.Entry<ImmutableBytesWritable, Aggregator[]> entry = hashIterator.next();
        ImmutableBytesWritable key = entry.getKey();
        Aggregator[] rowAggregators = entry.getValue();
        byte[] value = aggregators.toBytes(rowAggregators);
        Tuple tuple = wrapKeyValueAsResult(KeyValueUtil.newKeyValue(key, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, AGG_TIMESTAMP, value, 0, value.length));
        return tuple;
    }
    
    @Override
    public void close() throws SQLException {
        hashIterator = null;
        hash.clear();
        resultIterator.close();
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
    public String toString() {
        return "ClientHashAggregatingResultIterator [resultIterator=" 
            + resultIterator + ", aggregators=" + aggregators + ", groupByExpressions="
            + groupByExpressions + "]";
    }

    private ImmutableBytesWritable getGroupingKey(Tuple tuple, ImmutableBytesWritable ptr) throws SQLException {
        tuple.getKey(ptr);
        return ptr;
    }

    private Tuple wrapKeyValueAsResult(KeyValue keyValue) throws SQLException {
        return new SingleKeyValueTuple(keyValue);
    }

    private void populateHash() throws SQLException {
        ImmutableBytesWritable key = new ImmutableBytesWritable(UNITIALIZED_KEY_BUFFER);
        Aggregator[] rowAggregators;

        for (Tuple result = resultIterator.next(); result != null; result = resultIterator.next()) {
            key = getGroupingKey(result, key);
            rowAggregators = hash.get(key);
            if (rowAggregators == null) {
                rowAggregators = aggregators.newAggregators();
                hash.put(key, rowAggregators);
            }

            aggregators.aggregate(rowAggregators, result);
        }
    }
}
