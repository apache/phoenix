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
package org.apache.phoenix.execute;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.TupleProjector.ProjectedValueTuple;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.iterate.MappedByteBufferQueue;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.PhoenixParameterMetaData;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.JoinTableNode.JoinType;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.ValueBitSet;
import org.apache.phoenix.schema.KeyValueSchema.KeyValueSchemaBuilder;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ResultUtil;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.Lists;

public class SortMergeJoinPlan implements QueryPlan {
    private static final byte[] EMPTY_PTR = new byte[0];
    
    private final StatementContext context;
    private final FilterableStatement statement;
    private final TableRef table;
    private final JoinType type;
    private final QueryPlan lhsPlan;
    private final QueryPlan rhsPlan;
    private final List<Expression> lhsKeyExpressions;
    private final List<Expression> rhsKeyExpressions;
    private final KeyValueSchema joinedSchema;
    private final KeyValueSchema lhsSchema;
    private final KeyValueSchema rhsSchema;
    private final int rhsFieldPosition;
    private final boolean isSingleValueOnly;

    public SortMergeJoinPlan(StatementContext context, FilterableStatement statement, TableRef table, 
            JoinType type, QueryPlan lhsPlan, QueryPlan rhsPlan, List<Expression> lhsKeyExpressions, List<Expression> rhsKeyExpressions,
            PTable joinedTable, PTable lhsTable, PTable rhsTable, int rhsFieldPosition, boolean isSingleValueOnly) {
        if (type == JoinType.Right) throw new IllegalArgumentException("JoinType should not be " + type);
        this.context = context;
        this.statement = statement;
        this.table = table;
        this.type = type;
        this.lhsPlan = lhsPlan;
        this.rhsPlan = rhsPlan;
        this.lhsKeyExpressions = lhsKeyExpressions;
        this.rhsKeyExpressions = rhsKeyExpressions;
        this.joinedSchema = buildSchema(joinedTable);
        this.lhsSchema = buildSchema(lhsTable);
        this.rhsSchema = buildSchema(rhsTable);
        this.rhsFieldPosition = rhsFieldPosition;
        this.isSingleValueOnly = isSingleValueOnly;
    }

    private static KeyValueSchema buildSchema(PTable table) {
        KeyValueSchemaBuilder builder = new KeyValueSchemaBuilder(0);
        if (table != null) {
            for (PColumn column : table.getColumns()) {
                if (!SchemaUtil.isPKColumn(column)) {
                    builder.addField(column);
                }
            }
        }
        return builder.build();
    }

    @Override
    public ResultIterator iterator() throws SQLException {        
        return type == JoinType.Semi || type == JoinType.Anti ? 
                new SemiAntiJoinIterator(lhsPlan.iterator(), rhsPlan.iterator()) :
                new BasicJoinIterator(lhsPlan.iterator(), rhsPlan.iterator());
    }

    @Override
    public ExplainPlan getExplainPlan() throws SQLException {
        List<String> steps = Lists.newArrayList();
        steps.add("SORT-MERGE-JOIN (" + type.toString().toUpperCase() + ") TABLES");
        for (String step : lhsPlan.getExplainPlan().getPlanSteps()) {
            steps.add("    " + step);            
        }
        steps.add("AND" + (rhsSchema.getFieldCount() == 0 ? " (SKIP MERGE)" : ""));
        for (String step : rhsPlan.getExplainPlan().getPlanSteps()) {
            steps.add("    " + step);            
        }
        return new ExplainPlan(steps);
    }

    @Override
    public StatementContext getContext() {
        return context;
    }

    @Override
    public ParameterMetaData getParameterMetaData() {
        return PhoenixParameterMetaData.EMPTY_PARAMETER_META_DATA;
    }

    @Override
    public long getEstimatedSize() {
        return lhsPlan.getEstimatedSize() + rhsPlan.getEstimatedSize();
    }

    @Override
    public TableRef getTableRef() {
        return table;
    }

    @Override
    public RowProjector getProjector() {
        return null;
    }

    @Override
    public Integer getLimit() {
        return null;
    }

    @Override
    public OrderBy getOrderBy() {
        return null;
    }

    @Override
    public GroupBy getGroupBy() {
        return null;
    }

    @Override
    public List<KeyRange> getSplits() {
        return Collections.<KeyRange> emptyList();
    }

    @Override
    public List<List<Scan>> getScans() {
        return Collections.<List<Scan>> emptyList();
    }

    @Override
    public FilterableStatement getStatement() {
        return statement;
    }

    @Override
    public boolean isDegenerate() {
        return false;
    }

    @Override
    public boolean isRowKeyOrdered() {
        return false;
    }
    
    private class BasicJoinIterator implements ResultIterator {
        private final ResultIterator lhsIterator;
        private final ResultIterator rhsIterator;
        private boolean initialized;
        private Tuple lhsTuple;
        private Tuple rhsTuple;
        private JoinKey lhsKey;
        private JoinKey rhsKey;
        private Tuple nextLhsTuple;
        private Tuple nextRhsTuple;
        private JoinKey nextLhsKey;
        private JoinKey nextRhsKey;
        private ValueBitSet destBitSet;
        private ValueBitSet lhsBitSet;
        private ValueBitSet rhsBitSet;
        private byte[] emptyProjectedValue;
        private MappedByteBufferTupleQueue queue;
        private Iterator<Tuple> queueIterator;
        
        public BasicJoinIterator(ResultIterator lhsIterator, ResultIterator rhsIterator) {
            this.lhsIterator = lhsIterator;
            this.rhsIterator = rhsIterator;
            this.initialized = false;
            this.lhsTuple = null;
            this.rhsTuple = null;
            this.lhsKey = new JoinKey(lhsKeyExpressions);
            this.rhsKey = new JoinKey(rhsKeyExpressions);
            this.nextLhsTuple = null;
            this.nextRhsTuple = null;
            this.nextLhsKey = new JoinKey(lhsKeyExpressions);
            this.nextRhsKey = new JoinKey(rhsKeyExpressions);
            this.destBitSet = ValueBitSet.newInstance(joinedSchema);
            this.lhsBitSet = ValueBitSet.newInstance(lhsSchema);
            this.rhsBitSet = ValueBitSet.newInstance(rhsSchema);
            lhsBitSet.clear();
            int len = lhsBitSet.getEstimatedLength();
            this.emptyProjectedValue = new byte[len];
            lhsBitSet.toBytes(emptyProjectedValue, 0);
            int thresholdBytes = context.getConnection().getQueryServices().getProps().getInt(
                    QueryServices.SPOOL_THRESHOLD_BYTES_ATTRIB, QueryServicesOptions.DEFAULT_SPOOL_THRESHOLD_BYTES);
            this.queue = new MappedByteBufferTupleQueue(thresholdBytes);
            this.queueIterator = null;
        }
        
        @Override
        public void close() throws SQLException {
            lhsIterator.close();
            rhsIterator.close();
            queue.close();
        }

        @Override
        public Tuple next() throws SQLException {
            if (!initialized) {
                init();
            }

            Tuple next = null;
            while (next == null && !isEnd()) {
                if (queueIterator != null) {
                    if (queueIterator.hasNext()) {
                        next = join(lhsTuple, queueIterator.next());
                    } else {
                        boolean eq = nextLhsTuple != null && lhsKey.equals(nextLhsKey);
                        advance(true);
                        if (eq) {
                            queueIterator = queue.iterator();
                        } else {
                            queue.clear();
                            queueIterator = null;
                        }
                    }
                } else if (lhsTuple != null) {
                    if (rhsTuple != null) {
                        if (lhsKey.equals(rhsKey)) {
                            next = join(lhsTuple, rhsTuple);
                             if (nextLhsTuple != null && lhsKey.equals(nextLhsKey)) {
                                queue.offer(rhsTuple);
                                if (nextRhsTuple == null || !rhsKey.equals(nextRhsKey)) {
                                    queueIterator = queue.iterator();
                                    advance(true);
                                } else if (isSingleValueOnly) {
                                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.SINGLE_ROW_SUBQUERY_RETURNS_MULTIPLE_ROWS).build().buildException();
                                }
                            } else if (nextRhsTuple == null || !rhsKey.equals(nextRhsKey)) {
                                advance(true);
                            } else if (isSingleValueOnly) {
                                throw new SQLExceptionInfo.Builder(SQLExceptionCode.SINGLE_ROW_SUBQUERY_RETURNS_MULTIPLE_ROWS).build().buildException();                                
                            }
                            advance(false);
                        } else if (lhsKey.compareTo(rhsKey) < 0) {
                            if (type == JoinType.Full || type == JoinType.Left) {
                                next = join(lhsTuple, null);
                            }
                            advance(true);
                        } else {
                            if (type == JoinType.Full) {
                                next = join(null, rhsTuple);
                            }
                            advance(false);
                        }
                    } else { // left-join or full-join
                        next = join(lhsTuple, null);
                        advance(true);
                    }
                } else { // full-join
                    next = join(null, rhsTuple);
                    advance(false);
                }
            }

            return next;
        }

        @Override
        public void explain(List<String> planSteps) {
        }
        
        private void init() throws SQLException {
            nextLhsTuple = lhsIterator.next();
            if (nextLhsTuple != null) {
                nextLhsKey.evaluate(nextLhsTuple);
            }
            advance(true);
            nextRhsTuple = rhsIterator.next();
            if (nextRhsTuple != null) {
                nextRhsKey.evaluate(nextRhsTuple);
            }
            advance(false);
            initialized = true;
        }
        
        private void advance(boolean lhs) throws SQLException {
            if (lhs) {
                lhsTuple = nextLhsTuple;
                lhsKey.set(nextLhsKey);
                if (lhsTuple != null) {
                    nextLhsTuple = lhsIterator.next();
                    if (nextLhsTuple != null) {
                        nextLhsKey.evaluate(nextLhsTuple);
                    } else {
                        nextLhsKey.clear();
                    }
                }
            } else {
                rhsTuple = nextRhsTuple;
                rhsKey.set(nextRhsKey);
                if (rhsTuple != null) {
                    nextRhsTuple = rhsIterator.next();
                    if (nextRhsTuple != null) {
                        nextRhsKey.evaluate(nextRhsTuple);
                    } else {
                        nextRhsKey.clear();
                    }
                }                    
            }
        }
        
        private boolean isEnd() {
            return (lhsTuple == null && (rhsTuple == null || type != JoinType.Full))
                    || (queueIterator == null && rhsTuple == null && type == JoinType.Inner);
        }        
        
        private Tuple join(Tuple lhs, Tuple rhs) throws SQLException {
            try {
                ProjectedValueTuple t = null;
                if (lhs == null) {
                    t = new ProjectedValueTuple(rhs, rhs.getValue(0).getTimestamp(), 
                            this.emptyProjectedValue, 0, this.emptyProjectedValue.length, 
                            this.emptyProjectedValue.length);
                } else if (lhs instanceof ProjectedValueTuple) {
                    t = (ProjectedValueTuple) lhs;
                } else {
                    ImmutableBytesWritable ptr = context.getTempPtr();
                    TupleProjector.decodeProjectedValue(lhs, ptr);
                    lhsBitSet.clear();
                    lhsBitSet.or(ptr);
                    int bitSetLen = lhsBitSet.getEstimatedLength();
                    t = new ProjectedValueTuple(lhs, lhs.getValue(0).getTimestamp(), 
                            ptr.get(), ptr.getOffset(), ptr.getLength(), bitSetLen);
                    
                }
                return rhsBitSet == ValueBitSet.EMPTY_VALUE_BITSET ?
                        t : TupleProjector.mergeProjectedValue(
                                t, joinedSchema, destBitSet,
                                rhs, rhsSchema, rhsBitSet, rhsFieldPosition);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }
    }
    
    private class SemiAntiJoinIterator implements ResultIterator {
        private final ResultIterator lhsIterator;
        private final ResultIterator rhsIterator;
        private final boolean isSemi;
        private boolean initialized;
        private Tuple lhsTuple;
        private Tuple rhsTuple;
        private JoinKey lhsKey;
        private JoinKey rhsKey;
        
        public SemiAntiJoinIterator(ResultIterator lhsIterator, ResultIterator rhsIterator) {
            if (type != JoinType.Semi && type != JoinType.Anti) throw new IllegalArgumentException("Type " + type + " is not allowed by " + SemiAntiJoinIterator.class.getName());
            this.lhsIterator = lhsIterator;
            this.rhsIterator = rhsIterator;
            this.isSemi = type == JoinType.Semi;
            this.initialized = false;
            this.lhsTuple = null;
            this.rhsTuple = null;
            this.lhsKey = new JoinKey(lhsKeyExpressions);
            this.rhsKey = new JoinKey(rhsKeyExpressions);
        }

        @Override
        public void close() throws SQLException {
            lhsIterator.close();
            rhsIterator.close();
        }

        @Override
        public Tuple next() throws SQLException {
            if (!initialized) {
                advance(true);
                advance(false);
                initialized = true;
            }
            
            Tuple next = null;            
            while (lhsTuple != null && next == null) {
                if (rhsTuple != null) {
                    if (lhsKey.equals(rhsKey)) {
                        if (isSemi) {
                            next = lhsTuple;
                        }
                        advance(true);
                    } else if (lhsKey.compareTo(rhsKey) < 0) {
                        if (!isSemi) {
                            next = lhsTuple;
                        }
                        advance(true);
                    } else {
                        advance(false);
                    }
                } else {
                    if (!isSemi) {
                        next = lhsTuple;
                    }
                    advance(true);
                }
            }
            
            return next;
        }

        @Override
        public void explain(List<String> planSteps) {
        }
        
        private void advance(boolean lhs) throws SQLException {
            if (lhs) {
                lhsTuple = lhsIterator.next();
                if (lhsTuple != null) {
                    lhsKey.evaluate(lhsTuple);
                } else {
                    lhsKey.clear();
                }
            } else {
                rhsTuple = rhsIterator.next();
                if (rhsTuple != null) {
                    rhsKey.evaluate(rhsTuple);
                } else {
                    rhsKey.clear();
                }
            }
        }
    }
    
    private static class JoinKey implements Comparable<JoinKey> {
        private final List<Expression> expressions;
        private final List<ImmutableBytesWritable> keys;
        
        public JoinKey(List<Expression> expressions) {
            this.expressions = expressions;
            this.keys = Lists.newArrayListWithExpectedSize(expressions.size());
            for (int i = 0; i < expressions.size(); i++) {
                this.keys.add(new ImmutableBytesWritable());
            }
        }
        
        public void evaluate(Tuple tuple) {
            for (int i = 0; i < keys.size(); i++) {
                if (!expressions.get(i).evaluate(tuple, keys.get(i))) {
                    keys.get(i).set(EMPTY_PTR);
                }
            }
        }
        
        public void set(JoinKey other) {
            for (int i = 0; i < keys.size(); i++) {
                ImmutableBytesWritable key = other.keys.get(i);
                this.keys.get(i).set(key.get(), key.getOffset(), key.getLength());
            }            
        }
        
        public void clear() {
            for (int i = 0; i < keys.size(); i++) {
                this.keys.get(i).set(EMPTY_PTR);
            }            
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof JoinKey)) 
                return false;
            return this.compareTo((JoinKey) other) == 0;
        }
        
        @Override
        public int compareTo(JoinKey other) {
            for (int i = 0; i < keys.size(); i++) {
                int comp = this.keys.get(i).compareTo(other.keys.get(i));
                if (comp != 0) 
                    return comp; 
            }
            
            return 0;
        }
    }
    
    private static class MappedByteBufferTupleQueue extends MappedByteBufferQueue<Tuple> {

        public MappedByteBufferTupleQueue(int thresholdBytes) {
            super(thresholdBytes);
        }

        @Override
        protected MappedByteBufferSegmentQueue<Tuple> createSegmentQueue(
                int index, int thresholdBytes) {
            return new MappedByteBufferTupleSegmentQueue(index, thresholdBytes, false);
        }

        @Override
        protected Comparator<MappedByteBufferSegmentQueue<Tuple>> getSegmentQueueComparator() {
            return new Comparator<MappedByteBufferSegmentQueue<Tuple>>() {
                @Override
                public int compare(MappedByteBufferSegmentQueue<Tuple> q1, 
                        MappedByteBufferSegmentQueue<Tuple> q2) {
                    return q1.index() - q2.index();
                }                
            };
        }

        @Override
        public Iterator<Tuple> iterator() {
            return new Iterator<Tuple>() {
                private Iterator<MappedByteBufferSegmentQueue<Tuple>> queueIter;
                private Iterator<Tuple> currentIter;
                {
                    this.queueIter = getSegmentQueues().iterator();
                    this.currentIter = queueIter.hasNext() ? queueIter.next().iterator() : null;
                }
                
                @Override
                public boolean hasNext() {
                    return currentIter != null && currentIter.hasNext();
                }

                @Override
                public Tuple next() {
                    if (!hasNext())
                        return null;
                    
                    Tuple ret = currentIter.next();                    
                    if (!currentIter.hasNext()) {
                        this.currentIter = queueIter.hasNext() ? queueIter.next().iterator() : null;                       
                    }
                    
                    return ret;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
                
            };
        }
        
        private static class MappedByteBufferTupleSegmentQueue extends MappedByteBufferSegmentQueue<Tuple> {
            private LinkedList<Tuple> results;
            
            public MappedByteBufferTupleSegmentQueue(int index,
                    int thresholdBytes, boolean hasMaxQueueSize) {
                super(index, thresholdBytes, hasMaxQueueSize);
                this.results = Lists.newLinkedList();
            }

            @Override
            protected Queue<Tuple> getInMemoryQueue() {
                return results;
            }

            @Override
            protected int sizeOf(Tuple e) {
                KeyValue kv = KeyValueUtil.ensureKeyValue(e.getValue(0));
                return Bytes.SIZEOF_INT * 2 + kv.getLength();
            }

            @SuppressWarnings("deprecation")
            @Override
            protected void writeToBuffer(MappedByteBuffer buffer, Tuple e) {
                KeyValue kv = KeyValueUtil.ensureKeyValue(e.getValue(0));
                buffer.putInt(kv.getLength() + Bytes.SIZEOF_INT);
                buffer.putInt(kv.getLength());
                buffer.put(kv.getBuffer(), kv.getOffset(), kv.getLength());
            }

            @Override
            protected Tuple readFromBuffer(MappedByteBuffer buffer) {
                int length = buffer.getInt();
                if (length < 0)
                    return null;
                
                byte[] b = new byte[length];
                buffer.get(b);
                Result result = ResultUtil.toResult(new ImmutableBytesWritable(b));
                return new ResultTuple(result);
            }
            
        }
    }

}

