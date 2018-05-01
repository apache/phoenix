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

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.SingleAggregateFunction;
import org.apache.phoenix.schema.tuple.Tuple;

public class NonSizeTrackingServerAggregators extends ServerAggregators {
    public static final ServerAggregators EMPTY_AGGREGATORS = new NonSizeTrackingServerAggregators(new SingleAggregateFunction[0], new Aggregator[0], new Expression[0], 0);

    public NonSizeTrackingServerAggregators(SingleAggregateFunction[] functions, Aggregator[] aggregators,
            Expression[] expressions, int minNullableIndex) {
        super(functions, aggregators, expressions, minNullableIndex);
    }

    @Override
    public void aggregate(Aggregator[] aggregators, Tuple result) {
        for (int i = 0; i < expressions.length; i++) {
            if (expressions[i].evaluate(result, ptr) && ptr.getLength() != 0) {
                aggregators[i].aggregate(result, ptr);
            }
            expressions[i].reset();
        }
    }

}
