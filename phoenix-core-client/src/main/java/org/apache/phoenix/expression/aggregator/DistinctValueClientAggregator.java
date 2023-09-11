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
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;

public class DistinctValueClientAggregator extends DistinctValueWithCountClientAggregator {
    private final PDataType valueType;
    private final PDataType resultType;
    
    public DistinctValueClientAggregator(SortOrder sortOrder, PDataType valueType, PDataType resultType) {
        super(sortOrder);
        this.valueType = valueType;
        this.resultType = resultType;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (cachedResult == null) {            
            Object[] values = new Object[valueVsCount.size()];
            int i = 0;
            for (ImmutableBytesPtr key : valueVsCount.keySet()) {
                values[i++] = valueType.toObject(key, sortOrder);
            }
            cachedResult = PArrayDataType.instantiatePhoenixArray(valueType, values);
        }
        buffer = resultType.toBytes(cachedResult, sortOrder);
        ptr.set(buffer);
        return true;
    }

    @Override
    protected PDataType getResultDataType() {
        return resultType;
    }
    
    @Override
    protected int getBufferLength() {
        return 0;
    }

}
