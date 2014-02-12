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

import org.apache.phoenix.expression.function.SingleAggregateFunction;
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.KeyValueSchema.KeyValueSchemaBuilder;
import org.apache.phoenix.schema.ValueBitSet;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.SizedUtil;


/**
 * 
 * Represents an ordered list of Aggregators
 *
 * 
 * @since 0.1
 */
abstract public class Aggregators {
    protected final int estimatedByteSize;
    protected final KeyValueSchema schema;
    protected final ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    protected final ValueBitSet valueSet;
    protected final Aggregator[] aggregators;
    protected final SingleAggregateFunction[] functions;
    
    public int getEstimatedByteSize() {
        return estimatedByteSize;
    }
    
    public Aggregators(SingleAggregateFunction[] functions, Aggregator[] aggregators, int minNullableIndex) {
        this.functions = functions;
        this.aggregators = aggregators;
        this.estimatedByteSize = calculateSize(aggregators);
        this.schema = newValueSchema(aggregators, minNullableIndex);
        this.valueSet = ValueBitSet.newInstance(schema);
    }
    
    public KeyValueSchema getValueSchema() {
        return schema;
    }
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder(this.getClass().getName() + " [" + functions.length + "]:");
        for (int i = 0; i < functions.length; i++) {
            SingleAggregateFunction function = functions[i];
            buf.append("\t" + i + ") " + function );
        }
        return buf.toString();
    }
    
    /**
     * Return the aggregate functions
     */
    public SingleAggregateFunction[] getFunctions() {
        return functions;
    }
    
    /**
     * Aggregate over aggregators
     * @param result the single row Result from scan iteration
     */
    abstract public void aggregate(Aggregator[] aggregators, Tuple result);

    protected static int calculateSize(Aggregator[] aggregators) {
        
        int size = SizedUtil.ARRAY_SIZE /*aggregators[]*/  + (SizedUtil.POINTER_SIZE  * aggregators.length);
        for (Aggregator aggregator : aggregators) {
            size += aggregator.getSize();
        }
        return size;
    }
    
    /**
     * Get the ValueSchema for the Aggregators
     */
    private static KeyValueSchema newValueSchema(Aggregator[] aggregators, int minNullableIndex) {
        KeyValueSchemaBuilder builder = new KeyValueSchemaBuilder(minNullableIndex);
        for (int i = 0; i < aggregators.length; i++) {
            Aggregator aggregator = aggregators[i];
            builder.addField(aggregator);
        }
        return builder.build();
    }

    /**
     * @return byte representation of the ValueSchema
     */
    public byte[] toBytes(Aggregator[] aggregators) {
        return schema.toBytes(aggregators, valueSet, ptr);
    }
    
    public int getAggregatorCount() {
        return aggregators.length;
    }

    public Aggregator[] getAggregators() {
        return aggregators;
    }
    
    abstract public Aggregator[] newAggregators();
    
    public void reset(Aggregator[] aggregators) {
        for (int i = 0; i < aggregators.length; i++) {
            aggregators[i].reset();
        }
    }
    
    protected Aggregator getAggregator(int position) {
        return aggregators[position];
    }
}
