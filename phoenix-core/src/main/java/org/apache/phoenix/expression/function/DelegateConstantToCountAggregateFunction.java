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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.tuple.Tuple;


/**
 * 
 * Base class for non composite aggregation functions that optimize aggregation by
 * delegating to {@link CountAggregateFunction} when the child expression is a 
 * constant.
 *
 * 
 * @since 0.1
 */
abstract public class DelegateConstantToCountAggregateFunction extends SingleAggregateFunction {
    private static final ImmutableBytesWritable ZERO = new ImmutableBytesWritable(PLong.INSTANCE.toBytes(0L));
    private CountAggregateFunction delegate;
    
    public DelegateConstantToCountAggregateFunction() {
    }
    
    public DelegateConstantToCountAggregateFunction(List<Expression> childExpressions, CountAggregateFunction delegate) {
        super(childExpressions);
        // Using a delegate here causes us to optimize the number of aggregators
        // by sharing the CountAggregator across functions. On the server side,
        // this will always be null, since if it had not been null on the client,
        // the function would not have been transfered over (the delegate would
        // have instead).
        this.delegate = delegate;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (delegate == null) {
            return super.evaluate(tuple, ptr);
        }
        delegate.evaluate(tuple, ptr);
        if (PLong.INSTANCE.compareTo(ptr,ZERO) == 0) {
            return false;
        }
        return true;
    }


    @Override
    protected SingleAggregateFunction getDelegate() {
        return delegate != null ? delegate : super.getDelegate();
    }

}
