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

import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.schema.types.PInteger;

/**
 * Tuple that uses the 
 */
public class PositionBasedMultiKeyValueTuple extends BaseTuple {
    private BoundedSkipNullCellsList values;

    public PositionBasedMultiKeyValueTuple() {}
    
//    public PositionBasedMultiKeyValueTuple(List<Cell> values, int minQualifier, int maxQualifier) {
//        this.values = new BoundedSkipNullCellsList(minQualifier, maxQualifier);
//        setKeyValues(values);
//    }
    
//    public PositionBasedMultiKeyValueTuple(int minQualifier, int maxQualifier){
//        this.values = new BoundedSkipNullCellsList(minQualifier, maxQualifier);
//    }
    
    /** Caller must not modify the list that is passed here */
    @Override
    public void setKeyValues(List<Cell> values) {
        assert values instanceof BoundedSkipNullCellsList;
        this.values = (BoundedSkipNullCellsList)values;
    }

    @Override
    public void getKey(ImmutableBytesWritable ptr) {
        Cell value = values.getFirstCell();
        ptr.set(value.getRowArray(), value.getRowOffset(), value.getRowLength());
    }

    @Override
    public boolean isImmutable() {
        return true;
    }

    @Override
    public Cell getValue(byte[] family, byte[] qualifier) {
        return values.getCellForColumnQualifier((int)PInteger.INSTANCE.toObject(qualifier));
    }

    @Override
    public String toString() {
        return values.toString();
    }

    @Override
    public int size() {
        return values.size();
    }

    @Override
    public Cell getValue(int index) {
        return values.get(index);
    }

    @Override
    public boolean getValue(byte[] family, byte[] qualifier,
            ImmutableBytesWritable ptr) {
        Cell kv = getValue(family, qualifier);
        if (kv == null)
            return false;
        ptr.set(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength());
        return true;
    }}
