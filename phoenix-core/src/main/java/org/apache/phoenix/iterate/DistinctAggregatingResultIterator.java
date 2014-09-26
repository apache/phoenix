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
import java.util.*;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.schema.tuple.Tuple;

/**
 * Result scanner that dedups the incoming tuples to make them distinct.
 * <p>
 * Note that the results are held in memory
 *  
 * 
 * @since 1.2
 */
public class DistinctAggregatingResultIterator implements AggregatingResultIterator {
    private final AggregatingResultIterator delegate;
    private final RowProjector rowProjector;
    private Iterator<ResultEntry> resultIterator;
    private final ImmutableBytesWritable ptr1 = new ImmutableBytesWritable();
    private final ImmutableBytesWritable ptr2 = new ImmutableBytesWritable();

    private class ResultEntry {
        private final int hashCode;
        private final Tuple result;

        ResultEntry(Tuple result) {
            final int prime = 31;
            this.result = result;
            int hashCode = 0;
            for (ColumnProjector column : rowProjector.getColumnProjectors()) {
                Expression e = column.getExpression();
                if (e.evaluate(this.result, ptr1)) {
                    hashCode = prime * hashCode + ptr1.hashCode();
                }
            }
            this.hashCode = hashCode;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (o == null) {
                return false;
            }
            if (o.getClass() != this.getClass()) {
                return false;
            }
            ResultEntry that = (ResultEntry) o;
            for (ColumnProjector column : rowProjector.getColumnProjectors()) {
                Expression e = column.getExpression();
                boolean isNull1 = !e.evaluate(this.result, ptr1);
                boolean isNull2 = !e.evaluate(that.result, ptr2);
                if (isNull1 && isNull2) {
                    return true;
                }
                if (isNull1 || isNull2) {
                    return false;
                }
                if (ptr1.compareTo(ptr2) != 0) {
                    return false;
                }
            }
            return true;
        }
        
        @Override
        public int hashCode() {
            return hashCode;
        }
        
        Tuple getResult() {
            return result;
        }
    }
    
    protected ResultIterator getDelegate() {
        return delegate;
    }
    
    public DistinctAggregatingResultIterator(AggregatingResultIterator delegate,
            RowProjector rowProjector) {
        this.delegate = delegate;
        this.rowProjector = rowProjector;
    }

    @Override
    public Tuple next() throws SQLException {
        Iterator<ResultEntry> iterator = getResultIterator();
        if (iterator.hasNext()) {
            ResultEntry entry = iterator.next();
            Tuple tuple = entry.getResult();
            aggregate(tuple);
            return tuple;
        }
        resultIterator = Iterators.emptyIterator();
        return null;
    }
    
    private Iterator<ResultEntry> getResultIterator() throws SQLException {
        if (resultIterator != null) {
            return resultIterator;
        }
        
        Set<ResultEntry> entries = Sets.<ResultEntry>newHashSet(); // TODO: size?
        try {
            for (Tuple result = delegate.next(); result != null; result = delegate.next()) {
                ResultEntry entry = new ResultEntry(result);
                entries.add(entry);
            }
        } finally {
            delegate.close();
        }
        
        resultIterator = entries.iterator();
        return resultIterator;
    }

    @Override
    public void close()  {
        resultIterator = Iterators.emptyIterator();
    }


    @Override
    public void explain(List<String> planSteps) {
        delegate.explain(planSteps);
        planSteps.add("CLIENT DISTINCT ON " + rowProjector.toString());
    }

    @Override
    public void aggregate(Tuple result) {
        delegate.aggregate(result);
    }

	@Override
	public String toString() {
		return "DistinctAggregatingResultIterator [delegate=" + delegate
				+ ", rowProjector=" + rowProjector + ", resultIterator="
				+ resultIterator + ", ptr1=" + ptr1 + ", ptr2=" + ptr2 + "]";
	}
}
