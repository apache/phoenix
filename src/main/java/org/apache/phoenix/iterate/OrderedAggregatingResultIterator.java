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

import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.schema.tuple.Tuple;


/**
 * Result scanner that sorts aggregated rows by columns specified in the ORDER BY clause.
 * <p>
 * Note that currently the sort is entirely done in memory. 
 *  
 * 
 * @since 0.1
 */
public class OrderedAggregatingResultIterator extends OrderedResultIterator implements AggregatingResultIterator {

    public OrderedAggregatingResultIterator(AggregatingResultIterator delegate,
                                List<OrderByExpression> orderByExpressions,
                                int thresholdBytes, Integer limit) throws SQLException {
        super (delegate, orderByExpressions, thresholdBytes, limit);
    }

    @Override
    protected AggregatingResultIterator getDelegate() {
        return (AggregatingResultIterator)super.getDelegate();
    }
    
    @Override
    public Tuple next() throws SQLException {
        Tuple tuple = super.next();
        if (tuple != null) {
            aggregate(tuple);
        }
        return tuple;
    }
    
    @Override
    public void aggregate(Tuple result) {
        getDelegate().aggregate(result);
    }
}
