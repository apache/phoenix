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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.query.QueryConstants;

/**
 * When selecting specific columns in a SELECT query, this filter passes only selected columns
 * back to client.
 *
 * @since 3.0
 */
public class ColumnProjectionFilter extends FilterBase implements Writable {

    private byte[] emptyCFName;
    private Map<ImmutableBytesPtr, NavigableSet<ImmutableBytesPtr>> columnsTracker;
    private Set<byte[]> conditionOnlyCfs;

    public ColumnProjectionFilter() {

    }

    public ColumnProjectionFilter(byte[] emptyCFName,
            Map<ImmutableBytesPtr, NavigableSet<ImmutableBytesPtr>> columnsTracker,
            Set<byte[]> conditionOnlyCfs) {
        this.emptyCFName = emptyCFName;
        this.columnsTracker = columnsTracker;
        this.conditionOnlyCfs = conditionOnlyCfs;
    }

    public void readFields(DataInput input) throws IOException {
        this.emptyCFName = WritableUtils.readCompressedByteArray(input);
        int familyMapSize = WritableUtils.readVInt(input);
        assert familyMapSize > 0;
        columnsTracker = new TreeMap<ImmutableBytesPtr, NavigableSet<ImmutableBytesPtr>>();
        while (familyMapSize > 0) {
            byte[] cf = WritableUtils.readCompressedByteArray(input);
            int qualifiersSize = WritableUtils.readVInt(input);
            NavigableSet<ImmutableBytesPtr> qualifiers = null;
            if (qualifiersSize > 0) {
                qualifiers = new TreeSet<ImmutableBytesPtr>();
                while (qualifiersSize > 0) {
                    qualifiers.add(new ImmutableBytesPtr(WritableUtils.readCompressedByteArray(input)));
                    qualifiersSize--;
                }
            }
            columnsTracker.put(new ImmutableBytesPtr(cf), qualifiers);
            familyMapSize--;
        }
        int conditionOnlyCfsSize = WritableUtils.readVInt(input);
        this.conditionOnlyCfs = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
        while (conditionOnlyCfsSize > 0) {
            this.conditionOnlyCfs.add(WritableUtils.readCompressedByteArray(input));
            conditionOnlyCfsSize--;
        }
    }

    public void write(DataOutput output) throws IOException {
        WritableUtils.writeCompressedByteArray(output, this.emptyCFName);
        WritableUtils.writeVInt(output, this.columnsTracker.size());
        for (Entry<ImmutableBytesPtr, NavigableSet<ImmutableBytesPtr>> entry : this.columnsTracker.entrySet()) {
            // write family name
            WritableUtils.writeCompressedByteArray(output, entry.getKey().copyBytes());
            int qaulsSize = entry.getValue() == null ? 0 : entry.getValue().size();
            WritableUtils.writeVInt(output, qaulsSize);
            if (qaulsSize > 0) {
                for (ImmutableBytesPtr cq : entry.getValue()) {
                    // write qualifier name
                    WritableUtils.writeCompressedByteArray(output, cq.copyBytes());
                }
            }
        }
        // Write conditionOnlyCfs
        WritableUtils.writeVInt(output, this.conditionOnlyCfs.size());
        for (byte[] f : this.conditionOnlyCfs) {
            WritableUtils.writeCompressedByteArray(output, f);
        }
    }

    @Override
    public byte[] toByteArray() throws IOException {
        return Writables.getBytes(this);
    }
    
    public static ColumnProjectionFilter parseFrom(final byte [] pbBytes) throws DeserializationException {
        try {
            return (ColumnProjectionFilter)Writables.getWritable(pbBytes, new ColumnProjectionFilter());
        } catch (IOException e) {
            throw new DeserializationException(e);
        }
    }
    
    // "ptr" to be used for one time comparisons in filterRowCells
    private ImmutableBytesPtr ptr = new ImmutableBytesPtr();
    @Override
    public void filterRowCells(List<Cell> kvs) throws IOException {
        if (kvs.isEmpty()) return;
        Cell firstKV = kvs.get(0);
        Iterator<Cell> itr = kvs.iterator();
        while (itr.hasNext()) {
            Cell kv = itr.next();
            ptr.set(kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength());
            if (this.columnsTracker.containsKey(ptr)) {
                Set<ImmutableBytesPtr> cols = this.columnsTracker.get(ptr);
                ptr.set(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength());
                if (cols != null && !(cols.contains(ptr))) {
                    itr.remove();
                }
            } else {
                itr.remove();
            }
        }
        // make sure we're not holding to any of the byte[]'s
        ptr.set(HConstants.EMPTY_BYTE_ARRAY);
        if (kvs.isEmpty()) {
            kvs.add(new KeyValue(firstKV.getRowArray(), firstKV.getRowOffset(),firstKV.getRowLength(), this.emptyCFName,
                    0, this.emptyCFName.length, QueryConstants.EMPTY_COLUMN_BYTES, 0,
                    QueryConstants.EMPTY_COLUMN_BYTES.length, HConstants.LATEST_TIMESTAMP, Type.Maximum, null, 0, 0));
        }
    }

    @Override
    public boolean hasFilterRow() {
        return true;
    }

    @Override
    public boolean isFamilyEssential(byte[] name) {
        return conditionOnlyCfs.isEmpty() || this.conditionOnlyCfs.contains(name);
    }

    @Override
    public String toString() {
        return "";
    }
    
    @Override
    public ReturnCode filterKeyValue(Cell ignored) throws IOException {
      return ReturnCode.INCLUDE;
    }
}