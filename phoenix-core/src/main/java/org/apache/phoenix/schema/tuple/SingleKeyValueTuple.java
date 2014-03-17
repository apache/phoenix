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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;


public class SingleKeyValueTuple extends BaseTuple {
    private static final byte[] UNITIALIZED_KEY_BUFFER = new byte[0];
    private KeyValue keyValue;
    private ImmutableBytesWritable keyPtr = new ImmutableBytesWritable(UNITIALIZED_KEY_BUFFER);
    
    public SingleKeyValueTuple() {
    }
    
    public SingleKeyValueTuple(KeyValue keyValue) {
        if (keyValue == null) {
            throw new NullPointerException();
        }
        setKeyValue(keyValue);
    }
    
    public boolean hasKey() {
        return keyPtr.get() != UNITIALIZED_KEY_BUFFER;
    }
    
    public void reset() {
        this.keyValue = null;
        keyPtr.set(UNITIALIZED_KEY_BUFFER);
    }
    
    public void setKeyValue(KeyValue keyValue) {
        if (keyValue == null) {
            throw new IllegalArgumentException();
        }
        this.keyValue = keyValue;
        setKey(keyValue);
    }
    
    public void setKey(ImmutableBytesWritable ptr) {
        keyPtr.set(ptr.get(), ptr.getOffset(), ptr.getLength());
    }
    
    @SuppressWarnings("deprecation")
    public void setKey(KeyValue keyValue) {
        if (keyValue == null) {
            throw new IllegalArgumentException();
        }
        keyPtr.set(keyValue.getBuffer(), keyValue.getRowOffset(), keyValue.getRowLength());
    }
    
    @Override
    public void getKey(ImmutableBytesWritable ptr) {
        ptr.set(keyPtr.get(), keyPtr.getOffset(), keyPtr.getLength());
    }
    
    @Override
    public KeyValue getValue(byte[] cf, byte[] cq) {
        return keyValue;
    }

    @Override
    public boolean isImmutable() {
        return true;
    }
    
    @Override
    public String toString() {
        return "SingleKeyValueTuple[" + keyValue == null ? keyPtr.get() == UNITIALIZED_KEY_BUFFER ? "null" : Bytes.toStringBinary(keyPtr.get(),keyPtr.getOffset(),keyPtr.getLength()) : keyValue.toString() + "]";
    }

    @Override
    public int size() {
        return keyValue == null ? 0 : 1;
    }

    @Override
    public KeyValue getValue(int index) {
        if (index != 0 || keyValue == null) {
            throw new IndexOutOfBoundsException(Integer.toString(index));
        }
        return keyValue;
    }

    @SuppressWarnings("deprecation")
    @Override
    public boolean getValue(byte[] family, byte[] qualifier,
            ImmutableBytesWritable ptr) {
        if (keyValue == null)
            return false;
        ptr.set(keyValue.getBuffer(), keyValue.getValueOffset(), keyValue.getValueLength());
        return true;
    }
}
