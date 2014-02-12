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

import static org.apache.phoenix.query.QueryConstants.*;

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.Aggregators;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.KeyValueUtil;
import org.apache.phoenix.util.TupleUtil;



/**
 * 
 * Result scanner that aggregates the row count value for rows with duplicate keys.
 * The rows from the backing result iterator must be in key sorted order.  For example,
 * given the following input:
 *   a  1
 *   a  2
 *   b  1
 *   b  3
 *   c  1
 * the following will be output:
 *   a  3
 *   b  4
 *   c  1
 *
 * 
 * @since 0.1
 */
public class GroupedAggregatingResultIterator implements AggregatingResultIterator {
    private final ImmutableBytesWritable tempPtr = new ImmutableBytesWritable();
    private final PeekingResultIterator resultIterator;
    protected final Aggregators aggregators;
    
    public GroupedAggregatingResultIterator( PeekingResultIterator resultIterator, Aggregators aggregators) {
        if (resultIterator == null) throw new NullPointerException();
        if (aggregators == null) throw new NullPointerException();
        this.resultIterator = resultIterator;
        this.aggregators = aggregators;
    }
    
    @Override
    public Tuple next() throws SQLException {
        Tuple result = resultIterator.next();
        if (result == null) {
            return null;
        }
        Aggregator[] rowAggregators = aggregators.getAggregators();
        aggregators.reset(rowAggregators);
        while (true) {
            aggregators.aggregate(rowAggregators, result);
            Tuple nextResult = resultIterator.peek();
            if (nextResult == null || !TupleUtil.equals(result, nextResult, tempPtr)) {
                break;
            }
            result = resultIterator.next();
        }
        
        byte[] value = aggregators.toBytes(rowAggregators);
        result.getKey(tempPtr);
        return new SingleKeyValueTuple(KeyValueUtil.newKeyValue(tempPtr, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, AGG_TIMESTAMP, value, 0, value.length));
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
