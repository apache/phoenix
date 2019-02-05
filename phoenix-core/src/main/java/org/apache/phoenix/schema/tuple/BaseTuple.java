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


public abstract class BaseTuple implements Tuple {
    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean isImmutable() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void getKey(ImmutableBytesWritable ptr) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Cell mergeWithDynColsListBytesAndGetValue(int index, byte[] dynColsList) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Cell getValue(int index) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Cell getValue(byte [] family, byte [] qualifier) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean getValue(byte [] family, byte [] qualifier, ImmutableBytesWritable ptr) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getSequenceValue(int index) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void setKeyValues(List<Cell> values) {
        throw new UnsupportedOperationException();
    }
}
