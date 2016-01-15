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
import org.apache.hadoop.hbase.util.Bytes;


public class SingleKeyValueTuple extends BaseTuple {
    private static final byte[] UNITIALIZED_KEY_BUFFER = new byte[0];
    private Cell cell;
    private ImmutableBytesWritable keyPtr = new ImmutableBytesWritable(UNITIALIZED_KEY_BUFFER);
    
    public SingleKeyValueTuple() {
    }
    
    public SingleKeyValueTuple(Cell cell) {
        if (cell == null) {
            throw new NullPointerException();
        }
        setCell(cell);
    }
    
    public boolean hasKey() {
        return keyPtr.get() != UNITIALIZED_KEY_BUFFER;
    }
    
    public void reset() {
        this.cell = null;
        keyPtr.set(UNITIALIZED_KEY_BUFFER);
    }
    
    public void setCell(Cell cell) {
        if (cell == null) {
            throw new IllegalArgumentException();
        }
        this.cell = cell;
        setKey(cell);
    }
    
    public void setKey(ImmutableBytesWritable ptr) {
        keyPtr.set(ptr.get(), ptr.getOffset(), ptr.getLength());
    }
    
    public void setKey(Cell cell) {
        if (cell == null) {
            throw new IllegalArgumentException();
        }
        keyPtr.set(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
    }
    
    @Override
    public void getKey(ImmutableBytesWritable ptr) {
        ptr.set(keyPtr.get(), keyPtr.getOffset(), keyPtr.getLength());
    }
    
    @Override
    public Cell getValue(byte[] cf, byte[] cq) {
        return cell;
    }

    @Override
    public boolean isImmutable() {
        return true;
    }
    
    @Override
    public String toString() {
        return "SingleKeyValueTuple[" + cell == null ? keyPtr.get() == UNITIALIZED_KEY_BUFFER ? "null" : Bytes.toStringBinary(keyPtr.get(),keyPtr.getOffset(),keyPtr.getLength()) : cell.toString() + "]";
    }

    @Override
    public int size() {
        return cell == null ? 0 : 1;
    }

    @Override
    public Cell getValue(int index) {
        if (index != 0 || cell == null) {
            throw new IndexOutOfBoundsException(Integer.toString(index));
        }
        return cell;
    }

    @Override
    public boolean getValue(byte[] family, byte[] qualifier,
            ImmutableBytesWritable ptr) {
        if (cell == null)
            return false;
        ptr.set(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
        return true;
    }
}
