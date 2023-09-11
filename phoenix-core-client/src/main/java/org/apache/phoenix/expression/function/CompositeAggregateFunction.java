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

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;


/**
 * 
 * Base class for aggregation functions which are composed of other
 * aggregation functions (for example, AVG is modeled as a SUM aggregate
 * function and a COUNT aggregate function).
 *
 * 
 * @since 0.1
 */
abstract public class CompositeAggregateFunction extends AggregateFunction {

    public CompositeAggregateFunction(List<Expression> children) {
        super(children);
    }
    
    @Override
    public final <T> T accept(ExpressionVisitor<T> visitor) {
        return null;
    }
}
