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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.Aggregators;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.KeyValueUtil;
import org.apache.phoenix.util.ServerUtil;


/**
 * 
 * Client-side aggregate for key ordered aggregation. Prevents the comparison of
 * row keys for rows returned unless we cross a scan boundary.
 * 
 */
public class RowKeyOrderedAggregateResultIterator extends LookAheadResultIterator implements AggregatingResultIterator {
    private final ResultIterators resultIterators;
    private List<PeekingResultIterator> iterators;
    private final Aggregators aggregators;
    private final ImmutableBytesWritable currentKey = new ImmutableBytesWritable();
    private final ImmutableBytesWritable previousKey = new ImmutableBytesWritable();
    private boolean traversedIterator = true;
    private boolean nextTraversedIterators;
    private Tuple next;
    
   private int index;
    
    public RowKeyOrderedAggregateResultIterator(ResultIterators iterators, Aggregators aggregators) {
        this.resultIterators = iterators;
        this.aggregators = aggregators;
    }
    
    private List<PeekingResultIterator> getIterators() throws SQLException {
        if (iterators == null && resultIterators != null) {
            iterators = resultIterators.getIterators();
        }
        return iterators;
    }
    
    @Override
    public void close() throws SQLException {
        SQLException toThrow = null;
        try {
            if (resultIterators != null) {
                resultIterators.close();
            }
        } catch (Exception e) {
           toThrow = ServerUtil.parseServerException(e);
        } finally {
            try {
                if (iterators != null) {
                    for (;index < iterators.size(); index++) {
                        PeekingResultIterator iterator = iterators.get(index);
                        try {
                            iterator.close();
                        } catch (Exception e) {
                            if (toThrow == null) {
                                toThrow = ServerUtil.parseServerException(e);
                            } else {
                                toThrow.setNextException(ServerUtil.parseServerException(e));
                            }
                        }
                    }
                }
            } finally {
                if (toThrow != null) {
                    throw toThrow;
                }
            }
        }
    }


    @Override
    public void explain(List<String> planSteps) {
        if (resultIterators != null) {
            resultIterators.explain(planSteps);
        }
    }

    private Tuple nextTuple() throws SQLException {
        List<PeekingResultIterator> iterators = getIterators();
        while (index < iterators.size()) {
            PeekingResultIterator iterator = iterators.get(index);
            Tuple r = iterator.peek();
            if (r != null) {
                return iterator.next();
            }
            traversedIterator = true;
            iterator.close();
            index++;
        }
        return null;
    }
    
    private boolean continueAggregating(Tuple previous, Tuple next) {
        if (next == null) {
            return false;
        }
        next.getKey(currentKey);
        previous.getKey(previousKey);
        return (currentKey.compareTo(previousKey) == 0);
    }
    
    @Override
    public Tuple next() throws SQLException {
        Tuple t = super.next();
        if (t == null) {
            return null;
        }
        aggregate(t);
        return t;
    }
    
    @Override
    protected Tuple advance() throws SQLException {
        Tuple current = this.next;
        boolean traversedIterators = nextTraversedIterators;
        if (current == null) {
            current = nextTuple();
            traversedIterators = this.traversedIterator;
        }
        if (current != null) {
            Tuple previous = current;
            Aggregator[] rowAggregators = null;
            while (true) {
                current = nextTuple();
                if (!traversedIterators || !continueAggregating(previous, current)) {
                    break;
                }
                if (rowAggregators == null) {
                    rowAggregators = aggregate(previous);
                }
                aggregators.aggregate(rowAggregators, current); 
                traversedIterators = this.traversedIterator;
            }
            this.next = current;
            this.nextTraversedIterators = this.traversedIterator;
            if (rowAggregators == null) {
                current = previous;
            } else {
                byte[] value = aggregators.toBytes(rowAggregators);
                current = new SingleKeyValueTuple(KeyValueUtil.newKeyValue(previousKey, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, AGG_TIMESTAMP, value, 0, value.length));
            }
        }
        if (current == null) {
            close(); // Close underlying ResultIterators to free resources sooner rather than later
        }
        return current;
    }

	@Override
	public String toString() {
		return "RowKeyOrderedAggregateResultIterator [resultIterators=" + resultIterators + ", index=" + index + "]";
	}

    @Override
    public Aggregator[] aggregate(Tuple result) {
        Aggregator[] rowAggregators = aggregators.getAggregators();
        aggregators.reset(rowAggregators);
        aggregators.aggregate(rowAggregators, result);
        return rowAggregators;
    }
}
