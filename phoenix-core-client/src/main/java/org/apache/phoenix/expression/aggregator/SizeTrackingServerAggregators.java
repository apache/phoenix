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
import org.apache.phoenix.memory.MemoryManager.MemoryChunk;
import org.apache.phoenix.schema.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SizeTrackingServerAggregators extends ServerAggregators {
    private static final Logger LOGGER = LoggerFactory.getLogger(SizeTrackingServerAggregators.class);

    private final MemoryChunk chunk;
    private final int sizeIncrease;
    private long memoryUsed = 0;

    public SizeTrackingServerAggregators(SingleAggregateFunction[] functions, Aggregator[] aggregators,
            Expression[] expressions, int minNullableIndex, MemoryChunk chunk, int sizeIncrease) {
        super(functions, aggregators, expressions, minNullableIndex);
        this.chunk = chunk;
        this.sizeIncrease = sizeIncrease;
    }

    @Override
    public void aggregate(Aggregator[] aggregators, Tuple result) {
        long dsize = memoryUsed;
        for (int i = 0; i < expressions.length; i++) {
            if (expressions[i].evaluate(result, ptr) && ptr.getLength() != 0) {
                dsize -= aggregators[i].getSize();
                aggregators[i].aggregate(result, ptr);
                dsize += aggregators[i].getSize();
            }
            expressions[i].reset();
        }
        while(dsize > chunk.getSize()) {
            LOGGER.info("Request: {}, resizing {} by 1024*1024", dsize, chunk.getSize());
            chunk.resize(chunk.getSize() + sizeIncrease);
        }
        memoryUsed = dsize;
    }
    
}
