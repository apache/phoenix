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
package org.apache.phoenix.filter;

import java.io.IOException;

import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

import org.apache.phoenix.expression.Expression;


/**
 * 
 * SingleKeyValueComparisonFilter that needs to compare both the column family and
 * column qualifier parts of the key value to disambiguate with another similarly
 * named column qualifier in a different column family.
 *
 * 
 * @since 0.1
 */
public class SingleCFCQKeyValueComparisonFilter extends SingleKeyValueComparisonFilter {
    public SingleCFCQKeyValueComparisonFilter() {
    }

    public SingleCFCQKeyValueComparisonFilter(Expression expression) {
        super(expression);
    }

    @Override
    protected final int compare(byte[] cfBuf, int cfOffset, int cfLength, byte[] cqBuf, int cqOffset, int cqLength) {
        int c = Bytes.compareTo(cf, 0, cf.length, cfBuf, cfOffset, cfLength);
        if (c != 0) return c;
        return Bytes.compareTo(cq, 0, cq.length, cqBuf, cqOffset, cqLength);
    }
    
    public static SingleCFCQKeyValueComparisonFilter parseFrom(final byte [] pbBytes) throws DeserializationException {
        try {
            return (SingleCFCQKeyValueComparisonFilter)Writables.getWritable(pbBytes, new SingleCFCQKeyValueComparisonFilter());
        } catch (IOException e) {
            throw new DeserializationException(e);
        }
    }
}
