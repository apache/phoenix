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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PInteger;

public class PositionBasedResultTuple extends BaseTuple {
    private final BoundedSkipNullCellsList cells;
    
    //TODO: samarth see if we can get rid of this constructor altogether.
    public PositionBasedResultTuple(List<Cell> list) {
        checkArgument(list instanceof BoundedSkipNullCellsList, "Invalid list type");
        this.cells = (BoundedSkipNullCellsList)list;
    }
    
    @Override
    public void getKey(ImmutableBytesWritable ptr) {
        Cell value = cells.getFirstCell();
        ptr.set(value.getRowArray(), value.getRowOffset(), value.getRowLength());
    }

    @Override
    public boolean isImmutable() {
        return true;
    }

    @Override
    public KeyValue getValue(byte[] family, byte[] qualifier) {
        int columnQualifier = PInteger.INSTANCE.getCodec().decodeInt(qualifier, 0, SortOrder.ASC);
        return org.apache.hadoop.hbase.KeyValueUtil.ensureKeyValue(cells.getCellForColumnQualifier(columnQualifier));
    }

    //TODO: samarth implement this.
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("keyvalues=");
      return sb.toString();
    }

    @Override
    public int size() {
        return cells.size();
    }

    @Override
    public KeyValue getValue(int index) {
        return org.apache.hadoop.hbase.KeyValueUtil.ensureKeyValue(cells.get(index));
    }

    @Override
    public boolean getValue(byte[] family, byte[] qualifier,
            ImmutableBytesWritable ptr) {
        KeyValue kv = getValue(family, qualifier);
        if (kv == null)
            return false;
        ptr.set(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength());
        return true;
    }
}
