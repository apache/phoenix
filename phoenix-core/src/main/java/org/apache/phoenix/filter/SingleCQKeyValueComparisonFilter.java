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
 * SingleKeyValueComparisonFilter that needs to only compare the column qualifier
 * part of the key value since the column qualifier is unique across all column
 * families.
 *
 * 
 * @since 0.1
 */
public class SingleCQKeyValueComparisonFilter extends SingleKeyValueComparisonFilter {
    public SingleCQKeyValueComparisonFilter() {
    }

    public SingleCQKeyValueComparisonFilter(Expression expression) {
        super(expression);
    }

    @Override
    protected final int compare(byte[] cfBuf, int cfOffset, int cfLength, byte[] cqBuf, int cqOffset, int cqLength) {
        return Bytes.compareTo(cq, 0, cq.length, cqBuf, cqOffset, cqLength);
    }

    public static SingleCQKeyValueComparisonFilter parseFrom(final byte [] pbBytes) throws DeserializationException {
        try {
            return (SingleCQKeyValueComparisonFilter)Writables.getWritable(pbBytes, new SingleCQKeyValueComparisonFilter());
        } catch (IOException e) {
            throw new DeserializationException(e);
        }
    }
}
