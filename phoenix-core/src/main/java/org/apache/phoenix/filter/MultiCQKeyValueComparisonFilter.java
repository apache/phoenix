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
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;

/**
 *
 * Filter that evaluates WHERE clause expression, used in the case where there
 * are references to multiple column qualifiers over a single column family.
 *
 * 
 * @since 0.1
 */
public class MultiCQKeyValueComparisonFilter extends MultiKeyValueComparisonFilter {
    private ImmutableBytesPtr ptr = new ImmutableBytesPtr();
    private byte[] cf;

    public MultiCQKeyValueComparisonFilter() {
    }

    public MultiCQKeyValueComparisonFilter(Expression expression) {
        super(expression);
    }

    @Override
    protected Object setColumnKey(byte[] cf, int cfOffset, int cfLength, byte[] cq, int cqOffset,
            int cqLength) {
        ptr.set(cq, cqOffset, cqLength);
        return ptr;
    }

    @Override
    protected Object newColumnKey(byte[] cf, int cfOffset, int cfLength, byte[] cq, int cqOffset,
            int cqLength) {
        if (cfOffset == 0 && cf.length == cfLength) {
            this.cf = cf;
        } else {
            this.cf = new byte[cfLength];
            System.arraycopy(cf, cfOffset, this.cf, 0, cfLength);
        }
        return new ImmutableBytesPtr(cq, cqOffset, cqLength);
    }


    @SuppressWarnings("all") // suppressing missing @Override since this doesn't exist for HBase 0.94.4
    public boolean isFamilyEssential(byte[] name) {
        return Bytes.compareTo(cf, name) == 0;
    }
    
    public static MultiCQKeyValueComparisonFilter parseFrom(final byte [] pbBytes) throws DeserializationException {
        try {
            return (MultiCQKeyValueComparisonFilter)Writables.getWritable(pbBytes, new MultiCQKeyValueComparisonFilter());
        } catch (IOException e) {
            throw new DeserializationException(e);
        }
    }
}
