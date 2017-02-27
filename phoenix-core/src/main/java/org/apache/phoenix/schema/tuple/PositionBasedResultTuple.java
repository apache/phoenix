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

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.util.EncodedColumnsUtil;

public class PositionBasedResultTuple extends BaseTuple {
    private final EncodedColumnQualiferCellsList cells;
    
    public PositionBasedResultTuple(List<Cell> list) {
        checkArgument(list instanceof EncodedColumnQualiferCellsList, "Invalid list type");
        this.cells = (EncodedColumnQualiferCellsList)list;
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
        return org.apache.hadoop.hbase.KeyValueUtil.ensureKeyValue(cells.getCellForColumnQualifier(qualifier));
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("keyvalues=");
      if(this.cells == null || this.cells.isEmpty()) {
        sb.append("NONE");
        return sb.toString();
      }
      sb.append("{");
      boolean moreThanOne = false;
      for(Cell kv : this.cells) {
        if(moreThanOne) {
          sb.append(", \n");
        } else {
          moreThanOne = true;
        }
        sb.append(kv.toString()+"/value="+Bytes.toString(kv.getValueArray(), 
          kv.getValueOffset(), kv.getValueLength()));
      }
      sb.append("}\n");
      return sb.toString();
    }

    @Override
    public int size() {
        return cells.size();
    }

    @Override
    public KeyValue getValue(int index) {
        return org.apache.hadoop.hbase.KeyValueUtil.ensureKeyValue(index == 0 ? cells.getFirstCell() : cells.get(index));
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
    
    public Iterator<Cell> getTupleIterator() {
        return new TupleIterator(cells.iterator());
    }
    
    private static class TupleIterator implements Iterator<Cell> {
        
        private final Iterator<Cell> delegate;
        private TupleIterator(Iterator<Cell> delegate) {
            this.delegate = delegate;
        }
        
        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public Cell next() {
            return delegate.next();
        }

        @Override
        public void remove() {
            delegate.remove();
        }
        
    }
}
