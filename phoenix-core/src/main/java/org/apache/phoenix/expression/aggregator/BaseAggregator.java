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
package org.apache.phoenix.expression.aggregator;


import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.BaseTerminalExpression;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.util.SizedUtil;

import com.google.common.base.Preconditions;

/**
 * Base class for Aggregator implementations
 *
 * 
 * @since 0.1
 */
public abstract class BaseAggregator extends BaseTerminalExpression implements Aggregator {
    
    protected final SortOrder sortOrder;    
    
    public BaseAggregator(SortOrder sortOrder) {
    	Preconditions.checkNotNull(sortOrder);
        this.sortOrder = sortOrder;
    }
    
    @Override
    public boolean isNullable() {
        return true;
    }
    
    @Override
    public int getSize() {
        return SizedUtil.OBJECT_SIZE;
    }
    
    ImmutableBytesWritable evalClientAggs(Aggregator clientAgg) {
        CountAggregator ca = (CountAggregator)clientAgg;
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ca.evaluate(null, ptr);
        return ptr;
    }
    
    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return null;
    }

}
