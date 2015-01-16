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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkPositionIndex;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.SizedUtil;

/**
 * Result scanner that sorts aggregated rows by columns specified in the ORDER BY clause.
 * <p>
 * Note that currently the sort is entirely done in memory. 
 *  
 * 
 * @since 0.1
 */
public class OrderedResultIterator implements PeekingResultIterator {

    /** A container that holds pointers to a {@link Result} and its sort keys. */
    protected static class ResultEntry {
        protected final ImmutableBytesWritable[] sortKeys;
        protected final Tuple result;

        ResultEntry(ImmutableBytesWritable[] sortKeys, Tuple result) {
            this.sortKeys = sortKeys;
            this.result = result;
        }
        
        ImmutableBytesWritable getSortKey(int index) {
            checkPositionIndex(index, sortKeys.length);
            return sortKeys[index];
        }
        
        Tuple getResult() {
            return result;
        }
    }
    
    /** A function that returns Nth key for a given {@link ResultEntry}. */
    private static class NthKey implements Function<ResultEntry, ImmutableBytesWritable> {
        private final int index;

        NthKey(int index) {
            this.index = index;
        }
        @Override
        public ImmutableBytesWritable apply(ResultEntry entry) {
            return entry.getSortKey(index);
        }
    }

    /** Returns the expression of a given {@link OrderByExpression}. */
    private static final Function<OrderByExpression, Expression> TO_EXPRESSION = new Function<OrderByExpression, Expression>() {
        @Override
        public Expression apply(OrderByExpression column) {
            return column.getExpression();
        }
    };

    private final int thresholdBytes;
    private final Integer limit;
    private final ResultIterator delegate;
    private final List<OrderByExpression> orderByExpressions;
    private final long estimatedByteSize;
    
    private PeekingResultIterator resultIterator;
    private long byteSize;

    protected ResultIterator getDelegate() {
        return delegate;
    }
    
    public OrderedResultIterator(ResultIterator delegate,
                                 List<OrderByExpression> orderByExpressions,
                                 int thresholdBytes, Integer limit) {
        this(delegate, orderByExpressions, thresholdBytes, limit, 0);
    }

    public OrderedResultIterator(ResultIterator delegate,
            List<OrderByExpression> orderByExpressions, int thresholdBytes) throws SQLException {
        this(delegate, orderByExpressions, thresholdBytes, null);
    }

    public OrderedResultIterator(ResultIterator delegate, List<OrderByExpression> orderByExpressions, 
            int thresholdBytes, Integer limit, int estimatedRowSize) {
        checkArgument(!orderByExpressions.isEmpty());
        this.delegate = delegate;
        this.orderByExpressions = orderByExpressions;
        this.thresholdBytes = thresholdBytes;
        this.limit = limit;
        long estimatedEntrySize =
            // ResultEntry
            SizedUtil.OBJECT_SIZE + 
            // ImmutableBytesWritable[]
            SizedUtil.ARRAY_SIZE + orderByExpressions.size() * SizedUtil.IMMUTABLE_BYTES_WRITABLE_SIZE +
            // Tuple
            SizedUtil.OBJECT_SIZE + estimatedRowSize;

        // Make sure we don't overflow Long, though this is really unlikely to happen.
        assert(limit == null || Long.MAX_VALUE / estimatedEntrySize >= limit);

        this.estimatedByteSize = limit == null ? 0 : limit * estimatedEntrySize;
    }

    public Integer getLimit() {
        return limit;
    }

    public long getEstimatedByteSize() {
        return estimatedByteSize;
    }

    public long getByteSize() {
        return byteSize;
    }
    /**
     * Builds a comparator from the list of columns in ORDER BY clause.
     * @param orderByExpressions the columns in ORDER BY clause.
     * @return the comparator built from the list of columns in ORDER BY clause.
     */
    // ImmutableBytesWritable.Comparator doesn't implement generics
    @SuppressWarnings("unchecked")
    private static Comparator<ResultEntry> buildComparator(List<OrderByExpression> orderByExpressions) {
        Ordering<ResultEntry> ordering = null;
        int pos = 0;
        for (OrderByExpression col : orderByExpressions) {
            Ordering<ImmutableBytesWritable> o = Ordering.from(new ImmutableBytesWritable.Comparator());
            if(!col.isAscending()) o = o.reverse();
            o = col.isNullsLast() ? o.nullsLast() : o.nullsFirst();
            Ordering<ResultEntry> entryOrdering = o.onResultOf(new NthKey(pos++));
            ordering = ordering == null ? entryOrdering : ordering.compound(entryOrdering);
        }
        return ordering;
    }

    @Override
    public Tuple next() throws SQLException {
        return getResultIterator().next();
    }
    
    private PeekingResultIterator getResultIterator() throws SQLException {
        if (resultIterator != null) {
            return resultIterator;
        }
        
        final int numSortKeys = orderByExpressions.size();
        List<Expression> expressions = Lists.newArrayList(Collections2.transform(orderByExpressions, TO_EXPRESSION));
        final Comparator<ResultEntry> comparator = buildComparator(orderByExpressions);
        try{
            final MappedByteBufferSortedQueue queueEntries = new MappedByteBufferSortedQueue(comparator, limit, thresholdBytes);
            resultIterator = new PeekingResultIterator() {
                int count = 0;
                @Override
                public Tuple next() throws SQLException {
                    ResultEntry entry = queueEntries.poll();
                    if (entry == null || (limit != null && ++count > limit)) {
                        resultIterator.close();
                        resultIterator = PeekingResultIterator.EMPTY_ITERATOR;
                        return null;
                    }
                    return entry.getResult();
                }
                
                @Override
                public Tuple peek() throws SQLException {
                    if (limit != null && count > limit) {
                        return null;
                    }
                    ResultEntry entry =  queueEntries.peek();
                    if (entry == null) {
                        return null;
                    }
                    return entry.getResult();
                }

                @Override
                public void explain(List<String> planSteps) {
                }
                
                @Override
                public void close() throws SQLException {
                    queueEntries.close();
                }
            };
            for (Tuple result = delegate.next(); result != null; result = delegate.next()) {
                int pos = 0;
                ImmutableBytesWritable[] sortKeys = new ImmutableBytesWritable[numSortKeys];
                for (Expression expression : expressions) {
                    final ImmutableBytesWritable sortKey = new ImmutableBytesWritable();
                    boolean evaluated = expression.evaluate(result, sortKey);
                    // set the sort key that failed to get evaluated with null
                    sortKeys[pos++] = evaluated && sortKey.getLength() > 0 ? sortKey : null;
                }
                queueEntries.add(new ResultEntry(sortKeys, result));
            }
            this.byteSize = queueEntries.getByteSize();
        } catch (IOException e) {
            throw new SQLException("", e);
        } finally {
            delegate.close();
        }
        
        return resultIterator;
    }

    @Override
    public Tuple peek() throws SQLException {
        return getResultIterator().peek();
    }

    @Override
    public void close() throws SQLException {
        resultIterator.close();
        resultIterator = PeekingResultIterator.EMPTY_ITERATOR;
    }


    @Override
    public void explain(List<String> planSteps) {
        delegate.explain(planSteps);
        planSteps.add("CLIENT" + (limit == null ? "" : " TOP " + limit + " ROW"  + (limit == 1 ? "" : "S"))  + " SORTED BY " + orderByExpressions.toString());
    }

	@Override
	public String toString() {
		return "OrderedResultIterator [thresholdBytes=" + thresholdBytes
				+ ", limit=" + limit + ", delegate=" + delegate
				+ ", orderByExpressions=" + orderByExpressions
				+ ", estimatedByteSize=" + estimatedByteSize
				+ ", resultIterator=" + resultIterator + ", byteSize="
				+ byteSize + "]";
	}
}
