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

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.schema.tuple.Tuple;


/**
 * 
 * Interface to abstract the incremental calculation of an aggregated value.
 *
 * 
 * @since 0.1
 */
public interface Aggregator extends Expression {
    
    /**
     * Incrementally aggregate the value with the current row
     * @param tuple the result containing all the key values of the row
     * @param ptr the bytes pointer to the underlying result
     */
    public void aggregate(Tuple tuple, ImmutableBytesWritable ptr);
    
    /**
     * Get the size in bytes
     */
    public int getSize();
}
