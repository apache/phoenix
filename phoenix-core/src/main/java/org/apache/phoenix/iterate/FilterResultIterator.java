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
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.tuple.Tuple;


/**
 * 
 * Result scanner that filters out rows based on the results of a boolean
 * expression (i.e. filters out if {@link org.apache.phoenix.expression.Expression#evaluate(Tuple, ImmutableBytesWritable)}
 * returns false or the ptr contains a FALSE value}). May not be used where
 * the delegate provided is an {@link org.apache.phoenix.iterate.AggregatingResultIterator}.
 * For these, the {@link org.apache.phoenix.iterate.FilterAggregatingResultIterator} should be used.
 *
 * 
 * @since 0.1
 */
public class FilterResultIterator  extends LookAheadResultIterator {
    private final ResultIterator delegate;
    private final Expression expression;
    private final ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    
    public FilterResultIterator(ResultIterator delegate, Expression expression) {
        if (delegate instanceof AggregatingResultIterator) {
            throw new IllegalArgumentException("FilterResultScanner may not be used with an aggregate delegate. Use phoenix.iterate.FilterAggregateResultScanner instead");
        }
        this.delegate = delegate;
        this.expression = expression;
        if (expression.getDataType() != PBoolean.INSTANCE) {
            throw new IllegalArgumentException("FilterResultIterator requires a boolean expression, but got " + expression);
        }
    }

    @Override
    protected Tuple advance() throws SQLException {
        Tuple next;
        do {
            next = delegate.next();
            expression.reset();
        } while (next != null && (!expression.evaluate(next, ptr) || Boolean.FALSE.equals(expression.getDataType().toObject(ptr))));
        return next;
    }
    
    @Override
    public void close() throws SQLException {
        delegate.close();
    }

    @Override
    public void explain(List<String> planSteps) {
        delegate.explain(planSteps);
        planSteps.add("CLIENT FILTER BY " + expression.toString());
    }

	@Override
	public String toString() {
		return "FilterResultIterator [delegate=" + delegate + ", expression="
				+ expression + ", ptr=" + ptr + "]";
	}
}
