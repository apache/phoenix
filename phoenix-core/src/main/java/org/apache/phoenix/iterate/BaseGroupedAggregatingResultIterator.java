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
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.Aggregators;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.KeyValueUtil;

/**
 * 
 * Base class for result scanners that aggregate the row count value for rows with 
 * duplicate keys. This result scanner assumes that the results of the inner result 
 * scanner are returned in order of grouping keys.
 * 
 */
public abstract class BaseGroupedAggregatingResultIterator implements
        AggregatingResultIterator {
    private static final byte[] UNITIALIZED_KEY_BUFFER = new byte[0];
    protected final PeekingResultIterator resultIterator;
    protected final Aggregators aggregators;
    private ImmutableBytesWritable currentKey;
    private ImmutableBytesWritable nextKey;    

    public BaseGroupedAggregatingResultIterator(
            PeekingResultIterator resultIterator, Aggregators aggregators) {
        if (resultIterator == null) throw new NullPointerException();
        if (aggregators == null) throw new NullPointerException();
        this.resultIterator = resultIterator;
        this.aggregators = aggregators;
        this.currentKey = new ImmutableBytesWritable(UNITIALIZED_KEY_BUFFER);
        this.nextKey = new ImmutableBytesWritable(UNITIALIZED_KEY_BUFFER);        
    }
    
    protected abstract ImmutableBytesWritable getGroupingKey(Tuple tuple, ImmutableBytesWritable ptr) throws SQLException;
    protected abstract Tuple wrapKeyValueAsResult(KeyValue keyValue) throws SQLException;

    @Override
    public Tuple next() throws SQLException {
        Tuple result = resultIterator.next();
        if (result == null) {
            return null;
        }
        if (currentKey.get() == UNITIALIZED_KEY_BUFFER) {
            getGroupingKey(result, currentKey);
        }
        Aggregator[] rowAggregators = aggregators.getAggregators();
        aggregators.reset(rowAggregators);
        while (true) {
            aggregators.aggregate(rowAggregators, result);
            Tuple nextResult = resultIterator.peek();
            if (nextResult == null || !currentKey.equals(getGroupingKey(nextResult, nextKey))) {
                break;
            }
            result = resultIterator.next();
        }
        
        byte[] value = aggregators.toBytes(rowAggregators);
        Tuple tuple = wrapKeyValueAsResult(KeyValueUtil.newKeyValue(currentKey, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, AGG_TIMESTAMP, value, 0, value.length));
        currentKey.set(nextKey.get(), nextKey.getOffset(), nextKey.getLength());
        return tuple;
    }
    
    @Override
    public void close() throws SQLException {
        resultIterator.close();
    }
    
    @Override
    public void aggregate(Tuple result) {
        Aggregator[] rowAggregators = aggregators.getAggregators();
        aggregators.reset(rowAggregators);
        aggregators.aggregate(rowAggregators, result);
    }

    @Override
    public void explain(List<String> planSteps) {
        resultIterator.explain(planSteps);
    }

}
