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

import java.sql.SQLException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.aggregator.Aggregators;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;



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
public class GroupedAggregatingResultIterator extends BaseGroupedAggregatingResultIterator {

    public GroupedAggregatingResultIterator(PeekingResultIterator resultIterator, Aggregators aggregators) {
        super(resultIterator, aggregators);
    }

    @Override
    protected ImmutableBytesWritable getGroupingKey(Tuple tuple, ImmutableBytesWritable ptr) throws SQLException {
        tuple.getKey(ptr);
        return ptr;
    }

    @Override
    protected Tuple wrapKeyValueAsResult(KeyValue keyValue) throws SQLException {
        return new SingleKeyValueTuple(keyValue);
    }

	@Override
	public String toString() {
		return "GroupedAggregatingResultIterator [resultIterator=" 
		        + resultIterator + ", aggregators=" + aggregators + "]";
	}
}
