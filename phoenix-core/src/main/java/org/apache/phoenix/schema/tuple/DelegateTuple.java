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
package org.apache.phoenix.schema.tuple;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

public class DelegateTuple implements Tuple {
    private final Tuple delegate;
    
    public DelegateTuple(Tuple delegate) {
        this.delegate = delegate;
    }
    
    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isImmutable() {
        return delegate.isImmutable();
    }

    @Override
    public void getKey(ImmutableBytesWritable ptr) {
        delegate.getKey(ptr);
    }

    @Override
    public Cell getValue(int index) {
        return delegate.getValue(index);
    }

    @Override
    public Cell getValue(byte[] family, byte[] qualifier) {
        return delegate.getValue(family, qualifier);
    }

    @Override
    public boolean getValue(byte[] family, byte[] qualifier, ImmutableBytesWritable ptr) {
        return delegate.getValue(family, qualifier, ptr);
    }

    @Override
    public long getSequenceValue(int index) {
        return delegate.getSequenceValue(index);
    }
}
