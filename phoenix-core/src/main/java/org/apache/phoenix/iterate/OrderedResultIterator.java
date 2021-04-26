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

import static org.apache.phoenix.thirdparty.com.google.common.base.Preconditions.checkArgument;
import static org.apache.phoenix.thirdparty.com.google.common.base.Preconditions.checkPositionIndex;
import static org.apache.phoenix.util.ScanUtil.getDummyTuple;
import static org.apache.phoenix.util.ScanUtil.isDummy;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ExplainPlanAttributes
    .ExplainPlanAttributesBuilder;
import org.apache.phoenix.execute.DescVarLengthFastByteComparisons;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.SizedUtil;

import org.apache.phoenix.thirdparty.com.google.common.base.Function;
import org.apache.phoenix.thirdparty.com.google.common.collect.Collections2;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Ordering;

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

        static long sizeOf(ResultEntry e) {
          return sizeof(e.sortKeys) + sizeof(toKeyValues(e));
        }

        private static long sizeof(List<KeyValue> kvs) {
          long size = Bytes.SIZEOF_INT; // totalLen

          for (KeyValue kv : kvs) {
              size += kv.getLength();
              size += Bytes.SIZEOF_INT; // kv.getLength
          }

          return size;
        }

        private static long sizeof(ImmutableBytesWritable[] sortKeys) {
            long size = Bytes.SIZEOF_INT;
            if (sortKeys != null) {
                for (ImmutableBytesWritable sortKey : sortKeys) {
                    if (sortKey != null) {
                        size += sortKey.getLength();
                    }
                    size += Bytes.SIZEOF_INT;
                }
            }
            return size;
        }

        private static List<KeyValue> toKeyValues(ResultEntry entry) {
          Tuple result = entry.getResult();
          int size = result.size();
          List<KeyValue> kvs = new ArrayList<KeyValue>(size);
          for (int i = 0; i < size; i++) {
              kvs.add(PhoenixKeyValueUtil.maybeCopyCell(result.getValue(i)));
          }
          return kvs;
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

    private final boolean spoolingEnabled;
    private final long thresholdBytes;
    private final Integer limit;
    private final Integer offset;
    private final ResultIterator delegate;
    private final List<OrderByExpression> orderByExpressions;
    private final long estimatedByteSize;
    
    private PeekingResultIterator resultIterator;
    private boolean resultIteratorReady = false;
    private Tuple dummyTuple = null;
    private long byteSize;
    private long pageSizeMs;

    protected ResultIterator getDelegate() {
        return delegate;
    }
    
    public OrderedResultIterator(ResultIterator delegate, List<OrderByExpression> orderByExpressions,
            boolean spoolingEnabled, long thresholdBytes, Integer limit, Integer offset) {
        this(delegate, orderByExpressions, spoolingEnabled, thresholdBytes, limit, offset, 0, Long.MAX_VALUE);
    }

    public OrderedResultIterator(ResultIterator delegate, List<OrderByExpression> orderByExpressions,
            boolean spoolingEnabled, long thresholdBytes) throws SQLException {
        this(delegate, orderByExpressions, spoolingEnabled, thresholdBytes, null, null);
    }

    public OrderedResultIterator(ResultIterator delegate,
                                 List<OrderByExpression> orderByExpressions, boolean spoolingEnabled,
                                 long thresholdBytes, Integer limit, Integer offset, int estimatedRowSize) {
        this(delegate, orderByExpressions, spoolingEnabled, thresholdBytes, limit, offset, estimatedRowSize, Long.MAX_VALUE);
    }

    public OrderedResultIterator(ResultIterator delegate,
            List<OrderByExpression> orderByExpressions, boolean spoolingEnabled,
            long thresholdBytes, Integer limit, Integer offset, int estimatedRowSize, long pageSizeMs) {
        checkArgument(!orderByExpressions.isEmpty());
        this.delegate = delegate;
        this.orderByExpressions = orderByExpressions;
        this.spoolingEnabled = spoolingEnabled;
        this.thresholdBytes = thresholdBytes;
        this.offset = offset == null ? 0 : offset;
        if (limit != null) {
            this.limit = limit + this.offset;
        } else {
            this.limit = null;
        }
        long estimatedEntrySize =
            // ResultEntry
            SizedUtil.OBJECT_SIZE + 
            // ImmutableBytesWritable[]
            SizedUtil.ARRAY_SIZE + orderByExpressions.size() * SizedUtil.IMMUTABLE_BYTES_WRITABLE_SIZE +
            // Tuple
            SizedUtil.OBJECT_SIZE + estimatedRowSize;

        // Make sure we don't overflow Long, though this is really unlikely to happen.
        assert(limit == null || Long.MAX_VALUE / estimatedEntrySize >= limit + this.offset);

        // Both BufferedSortedQueue and SizeBoundQueue won't allocate more than thresholdBytes.
        this.estimatedByteSize = limit == null ? 0 : Math.min((limit + this.offset) * estimatedEntrySize, thresholdBytes);
        this.pageSizeMs = pageSizeMs;
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
            Expression e = col.getExpression();
            Comparator<ImmutableBytesWritable> comparator = 
                    e.getSortOrder() == SortOrder.DESC && !e.getDataType().isFixedWidth() 
                    ? buildDescVarLengthComparator() 
                    : new ImmutableBytesWritable.Comparator();
            Ordering<ImmutableBytesWritable> o = Ordering.from(comparator);
            if(!col.isAscending()) o = o.reverse();
            o = col.isNullsLast() ? o.nullsLast() : o.nullsFirst();
            Ordering<ResultEntry> entryOrdering = o.onResultOf(new NthKey(pos++));
            ordering = ordering == null ? entryOrdering : ordering.compound(entryOrdering);
        }
        return ordering;
    }

    /*
     * Same as regular comparator, but if all the bytes match and the length is
     * different, returns the longer length as bigger.
     */
    private static Comparator<ImmutableBytesWritable> buildDescVarLengthComparator() {
        return new Comparator<ImmutableBytesWritable>() {

            @Override
            public int compare(ImmutableBytesWritable o1, ImmutableBytesWritable o2) {
                return DescVarLengthFastByteComparisons.compareTo(
                        o1.get(), o1.getOffset(), o1.getLength(),
                        o2.get(), o2.getOffset(), o2.getLength());
            }
            
        };
    }
    
    @Override
    public Tuple next() throws SQLException {
        getResultIterator();
        if (!resultIteratorReady) {
            return dummyTuple;
        }
        return resultIterator.next();
    }
    
    private PeekingResultIterator getResultIterator() throws SQLException {
        if (resultIteratorReady) {
            // The results have not been ordered yet. When the results are ordered then the result iterator
            // will be ready to iterate over them
            return resultIterator;
        }
        
        final int numSortKeys = orderByExpressions.size();
        List<Expression> expressions = Lists.newArrayList(Collections2.transform(orderByExpressions, TO_EXPRESSION));
        final Comparator<ResultEntry> comparator = buildComparator(orderByExpressions);
        try{
            if (resultIterator == null) {
                resultIterator = new RecordPeekingResultIterator(PhoenixQueues.newResultEntrySortedQueue(comparator,
                        limit, spoolingEnabled, thresholdBytes));
            }
            final SizeAwareQueue<ResultEntry> queueEntries = ((RecordPeekingResultIterator)resultIterator).getQueueEntries();
            long startTime = EnvironmentEdgeManager.currentTimeMillis();
            for (Tuple result = delegate.next(); result != null; result = delegate.next()) {
                // result might be empty if it was filtered by a local index
                if (result.size() == 0) {
                    continue;
                }

                if (isDummy(result)) {
                    dummyTuple = result;
                    return resultIterator;
                }
                int pos = 0;
                ImmutableBytesWritable[] sortKeys = new ImmutableBytesWritable[numSortKeys];
                for (Expression expression : expressions) {
                    final ImmutableBytesWritable sortKey = new ImmutableBytesWritable();
                    boolean evaluated = expression.evaluate(result, sortKey);
                    // set the sort key that failed to get evaluated with null
                    sortKeys[pos++] = evaluated && sortKey.getLength() > 0 ? sortKey : null;
                }
                queueEntries.add(new ResultEntry(sortKeys, result));
                if (EnvironmentEdgeManager.currentTimeMillis() - startTime >= pageSizeMs) {
                    dummyTuple = getDummyTuple(result);
                    return resultIterator;
                }
            }
            resultIteratorReady = true;
            this.byteSize = queueEntries.getByteSize();
        } catch (IOException e) {
            ServerUtil.createIOException(e.getMessage(), e);
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
        // Guard against resultIterator being null
        if (null != resultIterator) {
            resultIterator.close();
        }
        resultIterator = PeekingResultIterator.EMPTY_ITERATOR;
    }


    @Override
    public void explain(List<String> planSteps) {
        delegate.explain(planSteps);
        planSteps.add("CLIENT" + (offset == null || offset == 0 ? "" : " OFFSET " + offset)
                + (limit == null ? "" : " TOP " + limit + " ROW" + (limit == 1 ? "" : "S")) + " SORTED BY "
                + orderByExpressions.toString());
    }

    @Override
    public void explain(List<String> planSteps,
            ExplainPlanAttributesBuilder explainPlanAttributesBuilder) {
        delegate.explain(planSteps, explainPlanAttributesBuilder);
        explainPlanAttributesBuilder.setClientOffset(offset);
        explainPlanAttributesBuilder.setClientRowLimit(limit);
        explainPlanAttributesBuilder.setClientSortedBy(
            orderByExpressions.toString());
        planSteps.add("CLIENT" + (offset == null || offset == 0 ? "" : " OFFSET " + offset)
            + (limit == null ? "" : " TOP " + limit + " ROW" + (limit == 1 ? "" : "S"))
            + " SORTED BY " + orderByExpressions.toString());
    }

    @Override
    public String toString() {
        return "OrderedResultIterator [thresholdBytes=" + thresholdBytes
                + ", limit=" + limit + ", offset=" + offset + ", delegate=" + delegate
                + ", orderByExpressions=" + orderByExpressions
                + ", estimatedByteSize=" + estimatedByteSize
                + ", resultIterator=" + resultIterator + ", byteSize="
                + byteSize + "]";
    }

    private class RecordPeekingResultIterator implements PeekingResultIterator {
        int count = 0;

        private SizeAwareQueue<ResultEntry> queueEntries;

        RecordPeekingResultIterator(SizeAwareQueue<ResultEntry> queueEntries){
            this.queueEntries = queueEntries;
        }

        public SizeAwareQueue<ResultEntry> getQueueEntries() {
            return queueEntries;
        }

        @Override
        public Tuple next() throws SQLException {
            ResultEntry entry = queueEntries.poll();
            while (entry != null && offset != null && count < offset) {
                count++;
                if (entry.getResult() == null) { return null; }
                entry = queueEntries.poll();
            }
            if (entry == null || (limit != null && count++ > limit)) {
                resultIterator.close();
                resultIterator = PeekingResultIterator.EMPTY_ITERATOR;
                return null;
            }
            return entry.getResult();
        }

        @Override
        public Tuple peek() throws SQLException {
            ResultEntry entry = queueEntries.peek();
            while (entry != null && offset != null && count < offset) {
                entry = queueEntries.poll();
                count++;
                if (entry == null) { return null; }
            }
            if (limit != null && count > limit) { return null; }
            entry = queueEntries.peek();
            if (entry == null) { return null; }
            return entry.getResult();
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
                queueEntries.close();
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }
    }
}
