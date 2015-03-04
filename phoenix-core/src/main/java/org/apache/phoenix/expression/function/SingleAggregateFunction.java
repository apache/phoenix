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
package org.apache.phoenix.expression.function;

import java.io.DataInput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;


/**
 * 
 * Base class for aggregate functions that calculate an aggregation
 * using a single {{@link Aggregator}
 *
 * 
 * @since 0.1
 */
abstract public class SingleAggregateFunction extends AggregateFunction {
    private static final List<Expression> DEFAULT_EXPRESSION_LIST = Arrays.<Expression>asList(LiteralExpression.newConstant(1, Determinism.ALWAYS));
    protected boolean isConstant;
    private Aggregator aggregator;
    
    /**
     * Sort aggregate functions with nullable fields last. This allows us not to have to store trailing null values.
     * Within non-nullable/nullable groups, put fixed width values first since we can access those more efficiently
     * (i.e. we can skip over groups of them in-mass instead of reading the length of each one to skip over as
     * required by a variable length value).
     */
    public static final Comparator<SingleAggregateFunction> SCHEMA_COMPARATOR = new Comparator<SingleAggregateFunction>() {

        @Override
        public int compare(SingleAggregateFunction o1, SingleAggregateFunction o2) {
            boolean isNullable1 = o1.isNullable();
            boolean isNullable2 = o2.isNullable();
            if (isNullable1 != isNullable2) {
                return isNullable1 ? 1 : -1;
            }
            isNullable1 = o1.getAggregatorExpression().isNullable();
            isNullable2 = o2.getAggregatorExpression().isNullable();
            if (isNullable1 != isNullable2) {
                return isNullable1 ? 1 : -1;
            }
            // Ensures COUNT(1) sorts first TODO: unit test for this
            boolean isConstant1 = o1.isConstantExpression();
            boolean isConstant2 = o2.isConstantExpression();
            if (isConstant1 != isConstant2) {
                return isConstant1 ? 1 : -1;
            }
            PDataType r1 = o1.getAggregator().getDataType();
            PDataType r2 = o2.getAggregator().getDataType();
            if (r1.isFixedWidth() != r2.isFixedWidth()) {
                return r1.isFixedWidth() ? -1 : 1;
            }
            return r1.compareTo(r2);
        }
    };
    
    protected SingleAggregateFunction() {
        this(DEFAULT_EXPRESSION_LIST, true);
    }

    public SingleAggregateFunction(List<Expression> children) {
        this(children, children.get(0) instanceof LiteralExpression);
    }
    
    private SingleAggregateFunction(List<Expression> children, boolean isConstant) {
        super(children);
        this.isConstant = children.get(0) instanceof LiteralExpression;
        this.aggregator = newClientAggregator();
    }

    public boolean isConstantExpression() {
        return isConstant;
    }
    
    @Override
    public PDataType getDataType() {
        return children.get(0).getDataType();
    }
    
    public Expression getAggregatorExpression() {
        return children.get(0);
    }
    
    public Aggregator getAggregator() {
        return aggregator;
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        return getAggregator().evaluate(tuple, ptr);
    }

    /**
     * Create the aggregator to do server-side aggregation.
     * The data type of the returned Aggregator must match
     * the data type returned by {@link #newClientAggregator()}
     * @param conf HBase configuration.
     * @return the aggregator to use on the server-side
     */
    abstract public Aggregator newServerAggregator(Configuration conf);
    /**
     * Create the aggregator to do client-side aggregation
     * based on the results returned from the aggregating
     * coprocessor. The data type of the returned Aggregator
     * must match the data type returned by {@link #newServerAggregator(Configuration)}
     * @return the aggregator to use on the client-side
     */
    public Aggregator newClientAggregator() {
        return newServerAggregator(null);
    }

    public Aggregator newServerAggregator(Configuration config, ImmutableBytesWritable ptr) {
        Aggregator agg = newServerAggregator(config);
        agg.aggregate(null, ptr);
        return agg;
    }
    
    public void readFields(DataInput input, Configuration conf) throws IOException {
        super.readFields(input);
        aggregator = newServerAggregator(conf);
    }

    @Override
    public boolean isNullable() {
        return true;
    }
    
    protected SingleAggregateFunction getDelegate() {
        return this;
    }

    @Override
    public final <T> T accept(ExpressionVisitor<T> visitor) {
        SingleAggregateFunction function = getDelegate();
        List<T> l = acceptChildren(visitor, visitor.visitEnter(function));
        T t = visitor.visitLeave(function, l);
        if (t == null) {
            t = visitor.defaultReturn(function, l);
        }
        return t;
    }

}
