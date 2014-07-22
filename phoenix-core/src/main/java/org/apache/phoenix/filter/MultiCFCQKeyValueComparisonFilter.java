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
 * Filter that evaluates WHERE clause expression, used in the case where there
 * are references to multiple column qualifiers over multiple column families.
 * Also there same qualifier names in different families.
 * 
 * @since 0.1
 */
public class MultiCFCQKeyValueComparisonFilter extends MultiKeyValueComparisonFilter {
    private final ImmutablePairBytesPtr ptr = new ImmutablePairBytesPtr();

    public MultiCFCQKeyValueComparisonFilter() {
    }

    public MultiCFCQKeyValueComparisonFilter(Expression expression) {
        super(expression);
    }

    @Override
    protected Object setColumnKey(byte[] cf, int cfOffset, int cfLength,
            byte[] cq, int cqOffset, int cqLength) {
        ptr.set(cf, cfOffset, cfLength, cq, cqOffset, cqLength);
        return ptr;
    }

    @Override
    protected Object newColumnKey(byte[] cf, int cfOffset, int cfLength,
            byte[] cq, int cqOffset, int cqLength) {

        byte[] cfKey;
        if (cfOffset == 0 && cf.length == cfLength) {
            cfKey = cf;
        } else {
            // Copy bytes here, but figure cf names are typically a few bytes at most,
            // so this will be better than creating an ImmutableBytesPtr
            cfKey = new byte[cfLength];
            System.arraycopy(cf, cfOffset, cfKey, 0, cfLength);
        }
        cfSet.add(cfKey);
        return new ImmutablePairBytesPtr(cf, cfOffset, cfLength, cq, cqOffset, cqLength);
    }

    private static class ImmutablePairBytesPtr {
        private byte[] bytes1;
        private int offset1;
        private int length1;
        private byte[] bytes2;
        private int offset2;
        private int length2;
        private int hashCode;

        private ImmutablePairBytesPtr() {
        }

        private ImmutablePairBytesPtr(byte[] bytes1, int offset1, int length1, byte[] bytes2, int offset2, int length2) {
            set(bytes1, offset1, length1, bytes2, offset2, length2);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        public void set(byte[] bytes1, int offset1, int length1, byte[] bytes2, int offset2, int length2) {
            this.bytes1 = bytes1;
            this.offset1 = offset1;
            this.length1 = length1;
            this.bytes2 = bytes2;
            this.offset2 = offset2;
            this.length2 = length2;
            int hash = 1;
            for (int i = offset1; i < offset1 + length1; i++)
                hash = (31 * hash) + bytes1[i];
            for (int i = offset2; i < offset2 + length2; i++)
                hash = (31 * hash) + bytes2[i];
            hashCode = hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            ImmutablePairBytesPtr that = (ImmutablePairBytesPtr)obj;
            if (this.hashCode != that.hashCode) return false;
            if (Bytes.compareTo(this.bytes2, this.offset2, this.length2, that.bytes2, that.offset2, that.length2) != 0) return false;
            if (Bytes.compareTo(this.bytes1, this.offset1, this.length1, that.bytes1, that.offset1, that.length1) != 0) return false;
            return true;
        }
    }

    public static MultiCFCQKeyValueComparisonFilter parseFrom(final byte [] pbBytes) throws DeserializationException {
        try {
            return (MultiCFCQKeyValueComparisonFilter)Writables.getWritable(pbBytes, new MultiCFCQKeyValueComparisonFilter());
        } catch (IOException e) {
            throw new DeserializationException(e);
        }
    }
}
