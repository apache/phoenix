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
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.schema.tuple.Tuple;

/**
 * 
 * ResultIterator that does a merge sort on the list of iterators provided,
 * returning the rows ordered by the OrderByExpression. The input
 * iterators must be ordered by the OrderByExpression.
 *
 */
public class MergeSortTopNResultIterator extends MergeSortResultIterator {

    private final int limit;
    private int count = 0;
    private final List<OrderByExpression> orderByColumns;
    private final ImmutableBytesWritable ptr1 = new ImmutableBytesWritable();
    private final ImmutableBytesWritable ptr2 = new ImmutableBytesWritable();
    
    public MergeSortTopNResultIterator(ResultIterators iterators, Integer limit, List<OrderByExpression> orderByColumns) {
        super(iterators);
        this.limit = limit == null ? -1 : limit;
        this.orderByColumns = orderByColumns;
    }

    @Override
    protected int compare(Tuple t1, Tuple t2) {
        for (int i = 0; i < orderByColumns.size(); i++) {
            OrderByExpression order = orderByColumns.get(i);
            Expression orderExpr = order.getExpression();
            boolean isNull1 = !orderExpr.evaluate(t1, ptr1) || ptr1.getLength() == 0;
            boolean isNull2 = !orderExpr.evaluate(t2, ptr2) || ptr2.getLength() == 0;
            if (isNull1 && isNull2) {
                continue;
            } else if (isNull1) {
                return order.isNullsLast() ? 1 : -1;
            } else if (isNull2) {
                return order.isNullsLast() ? -1 : 1;
            }
            int cmp = ptr1.compareTo(ptr2);
            if (cmp == 0) {
                continue;
            }
            return order.isAscending() ? cmp : -cmp;
        }
        return 0;
    }

    @Override
    public Tuple peek() throws SQLException {
        if (limit >= 0 && count >= limit) {
            return null;
        }
        return super.peek();
    }

    @Override
    public Tuple next() throws SQLException {
        if (limit >= 0 && count++ >= limit) {
            return null;
        }
        return super.next();
    }


    @Override
    public void explain(List<String> planSteps) {
        resultIterators.explain(planSteps);
        planSteps.add("CLIENT MERGE SORT");
    }

	@Override
	public String toString() {
		return "MergeSortTopNResultIterator [limit=" + limit + ", count="
				+ count + ", orderByColumns=" + orderByColumns + ", ptr1="
				+ ptr1 + ", ptr2=" + ptr2 + "]";
	}
}
