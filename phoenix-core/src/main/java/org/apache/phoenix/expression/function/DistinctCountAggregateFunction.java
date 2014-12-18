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

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.DistinctCountClientAggregator;
import org.apache.phoenix.expression.aggregator.DistinctValueWithCountServerAggregator;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.SchemaUtil;


/**
 * 
 * Built-in function for COUNT(distinct <expression>) aggregate function,
 *
 * 
 * @since 1.2.1
 */
@BuiltInFunction(name=DistinctCountAggregateFunction.NAME, args= {@Argument()} )
public class DistinctCountAggregateFunction extends DelegateConstantToCountAggregateFunction {
    public static final String NAME = "DISTINCT_COUNT";
    public static final String NORMALIZED_NAME = SchemaUtil.normalizeIdentifier(NAME);
    public final static byte[] ZERO = PLong.INSTANCE.toBytes(0L);
    public final static byte[] ONE = PLong.INSTANCE.toBytes(1L);
    
    public DistinctCountAggregateFunction() {
    }

    public DistinctCountAggregateFunction(List<Expression> childExpressions) {
        this(childExpressions, null);
    }

    public DistinctCountAggregateFunction(List<Expression> childExpressions,
            CountAggregateFunction delegate) {
        super(childExpressions, delegate);
        assert childExpressions.size() == 1;
    }
    
    @Override
    public int hashCode() {
        return isConstantExpression() ? 0 : super.hashCode();
    }

    /**
     * The COUNT function never returns null
     */
    @Override
    public boolean isNullable() {
        return false;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        DistinctCountAggregateFunction other = (DistinctCountAggregateFunction)obj;
        return (isConstantExpression() && other.isConstantExpression()) || children.equals(other.getChildren());
    }

    @Override
    public PDataType getDataType() {
        return PLong.INSTANCE;
    }

    @Override 
    public DistinctCountClientAggregator newClientAggregator() {
        return new DistinctCountClientAggregator(getAggregatorExpression().getSortOrder());
    }
    
    @Override 
    public Aggregator newServerAggregator(Configuration conf) {
        return new DistinctValueWithCountServerAggregator(conf);
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        // TODO: optimize query plan of this to run scan serially for a limit of one row
        if (!super.evaluate(tuple, ptr)) {
            ptr.set(ZERO); // If evaluate returns false, then no rows were found, so result is 0
        } else if (isConstantExpression()) {
            ptr.set(ONE); // Otherwise, we found one or more rows, so a distinct on a constant is 1
        }
        return true; // Always evaluates to a LONG value
    }
    
    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Aggregator newServerAggregator(Configuration config, ImmutableBytesWritable ptr) {
        DistinctCountClientAggregator clientAgg = newClientAggregator();
        clientAgg.aggregate(null, ptr);
        return new DistinctValueWithCountServerAggregator(config, clientAgg);
    }
}
