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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.expression.function.SingleAggregateFunction;
import org.apache.phoenix.memory.MemoryManager.MemoryChunk;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.tuple.Tuple;


/**
 * 
 * Aggregators that execute on the server-side
 *
 */
public abstract class ServerAggregators extends Aggregators {
    protected final Expression[] expressions;
    
    protected ServerAggregators(SingleAggregateFunction[] functions, Aggregator[] aggregators, Expression[] expressions, int minNullableIndex) {
        super(functions, aggregators, minNullableIndex);
        if (aggregators.length != expressions.length) {
            throw new IllegalArgumentException("Number of aggregators (" + aggregators.length 
                    + ") must match the number of expressions (" + Arrays.toString(expressions) + ")");
        }
        this.expressions = expressions;
    }
    
    @Override
    public abstract void aggregate(Aggregator[] aggregators, Tuple result);
    
    /**
     * Serialize an Aggregator into a byte array
     * @param aggFuncs list of aggregator to serialize
     * @return serialized byte array respresentation of aggregator
     */
    public static byte[] serialize(List<SingleAggregateFunction> aggFuncs, int minNullableIndex) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            DataOutputStream output = new DataOutputStream(stream);
            WritableUtils.writeVInt(output, minNullableIndex);
            WritableUtils.writeVInt(output, aggFuncs.size());
            for (int i = 0; i < aggFuncs.size(); i++) {
                SingleAggregateFunction aggFunc = aggFuncs.get(i);
                WritableUtils.writeVInt(output, ExpressionType.valueOf(aggFunc).ordinal());
                aggFunc.write(output);
            }
            return stream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Aggregator[] newAggregators() {
        return newAggregators(null);
    }

    public Aggregator[] newAggregators(Configuration conf) {
        Aggregator[] aggregators = new Aggregator[functions.length];
        for (int i = 0; i < functions.length; i++) {
            aggregators[i] = functions[i].newServerAggregator(conf);
        }
        return aggregators;
    }

    /**
     * Deserialize aggregators from the serialized byte array representation
     * @param b byte array representation of a list of Aggregators
     * @param conf Server side configuration used by HBase
     * @return newly instantiated Aggregators instance
     */
    public static ServerAggregators deserialize(byte[] b, Configuration conf, MemoryChunk chunk) {
        if (b == null) {
            return NonSizeTrackingServerAggregators.EMPTY_AGGREGATORS;
        }
        ByteArrayInputStream stream = new ByteArrayInputStream(b);
        try {
            DataInputStream input = new DataInputStream(stream);
            int minNullableIndex = WritableUtils.readVInt(input);
            int len = WritableUtils.readVInt(input);
            Aggregator[] aggregators = new Aggregator[len];
            Expression[] expressions = new Expression[len];
            SingleAggregateFunction[] functions = new SingleAggregateFunction[len];
            for (int i = 0; i < aggregators.length; i++) {
                SingleAggregateFunction aggFunc = (SingleAggregateFunction)ExpressionType.values()[WritableUtils.readVInt(input)].newInstance();
                aggFunc.readFields(input, conf);
                functions[i] = aggFunc;
                aggregators[i] = aggFunc.getAggregator();
                expressions[i] = aggFunc.getAggregatorExpression();
            }
            boolean trackSize = false;
            if (chunk != null) {
                for (Aggregator aggregator : aggregators) {
                    if (aggregator.trackSize()) {
                        trackSize = true;
                        break;
                    }
                }
            }
            return trackSize ?
                    new SizeTrackingServerAggregators(functions, aggregators,expressions, minNullableIndex, chunk, 
                            conf.getInt(QueryServices.AGGREGATE_CHUNK_SIZE_INCREASE_ATTRIB, 
                                    QueryServicesOptions.DEFAULT_AGGREGATE_CHUNK_SIZE_INCREASE)) :
                    new NonSizeTrackingServerAggregators(functions, aggregators,expressions, minNullableIndex);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
