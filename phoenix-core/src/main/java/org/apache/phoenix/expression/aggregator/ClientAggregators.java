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

import java.util.List;

import org.apache.phoenix.expression.function.SingleAggregateFunction;
import org.apache.phoenix.schema.ValueBitSet;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.TupleUtil;



/**
 * 
 * Aggregators that execute on the client-side
 *
 * 
 * @since 0.1
 */
public class ClientAggregators extends Aggregators {
    private final ValueBitSet tempValueSet; 
  
    private static Aggregator[] getAggregators(List<SingleAggregateFunction> aggFuncs) {
        Aggregator[] aggregators = new Aggregator[aggFuncs.size()];
        for (int i = 0; i < aggregators.length; i++) {
            aggregators[i] = aggFuncs.get(i).getAggregator();
        }
        return aggregators;
    }
    
    public ClientAggregators(List<SingleAggregateFunction> functions, int minNullableIndex) {
        super(functions.toArray(new SingleAggregateFunction[functions.size()]), getAggregators(functions), minNullableIndex);
        this.tempValueSet = ValueBitSet.newInstance(schema);
    }
    
    @Override
    public void aggregate(Aggregator[] aggregators, Tuple result) {
        TupleUtil.getAggregateValue(result, ptr);
        tempValueSet.clear();
        tempValueSet.or(ptr);

        int i = 0, maxOffset = ptr.getOffset() + ptr.getLength();
        Boolean hasValue;
        schema.iterator(ptr);
        while ((hasValue=schema.next(ptr, i, maxOffset, tempValueSet)) != null) {
            if (hasValue) {
                aggregators[i].aggregate(result, ptr);
            }
            i++;
        }
    }
    
    @Override
    public Aggregator[] newAggregators() {
        Aggregator[] aggregators = new Aggregator[functions.length];
        for (int i = 0; i < functions.length; i++) {
            aggregators[i] = functions[i].newClientAggregator();
        }
        return aggregators;
    }
    
}
