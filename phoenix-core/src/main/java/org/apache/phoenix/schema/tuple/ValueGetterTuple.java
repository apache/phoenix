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

import static org.apache.phoenix.hbase.index.util.ImmutableBytesPtr.copyBytesIfNecessary;

import java.io.IOException;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;

/**
 * 
 * Class used to construct a {@link Tuple} in order to evaluate an {@link Expression}
 */
public class ValueGetterTuple extends BaseTuple {
	private ValueGetter valueGetter;
    
    public ValueGetterTuple(ValueGetter valueGetter) {
        this.valueGetter = valueGetter;
    }
    
    public ValueGetterTuple() {
    }
    
    @Override
    public void getKey(ImmutableBytesWritable ptr) {
        ptr.set(valueGetter.getRowKey());
    }

    @Override
    public boolean isImmutable() {
        return true;
    }

    @Override
    public KeyValue getValue(byte[] family, byte[] qualifier) {
    	ImmutableBytesPtr value = null;
        try {
            value = valueGetter.getLatestValue(new ColumnReference(family, qualifier));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    	return new KeyValue(valueGetter.getRowKey(), family, qualifier, HConstants.LATEST_TIMESTAMP, Type.Put, value!=null? copyBytesIfNecessary(value) : null);
    }

    @Override
    public String toString() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyValue getValue(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getValue(byte[] family, byte[] qualifier,
            ImmutableBytesWritable ptr) {
        KeyValue kv = getValue(family, qualifier);
        if (kv == null)
            return false;
        ptr.set(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
        return true;
    }

}
