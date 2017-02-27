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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.phoenix.query.QueryConstants.ENCODED_EMPTY_COLUMN_BYTES;
import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

public class EncodedQualifiersColumnProjectionFilter extends FilterBase implements Writable {

    private byte[] emptyCFName;
    private BitSet trackedColumns;
    private QualifierEncodingScheme encodingScheme;
    private Set<byte[]> conditionOnlyCfs;
    
    public EncodedQualifiersColumnProjectionFilter() {}

    public EncodedQualifiersColumnProjectionFilter(byte[] emptyCFName, BitSet trackedColumns, Set<byte[]> conditionCfs, QualifierEncodingScheme encodingScheme) {
        checkArgument(encodingScheme != NON_ENCODED_QUALIFIERS, "Filter can only be used for encoded qualifiers");
        this.emptyCFName = emptyCFName;
        this.trackedColumns = trackedColumns;
        this.encodingScheme = encodingScheme;
        this.conditionOnlyCfs = conditionCfs;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.emptyCFName = WritableUtils.readCompressedByteArray(input);
        int bitsetLongArraySize = WritableUtils.readVInt(input);
        long[] bitsetLongArray = new long[bitsetLongArraySize];
        for (int i = 0; i < bitsetLongArraySize; i++) {
            bitsetLongArray[i] = WritableUtils.readVLong(input);
        }
        this.trackedColumns = BitSet.valueOf(bitsetLongArray);
        this.encodingScheme = QualifierEncodingScheme.values()[WritableUtils.readVInt(input)];
        int conditionOnlyCfsSize = WritableUtils.readVInt(input);
        this.conditionOnlyCfs = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
        while (conditionOnlyCfsSize > 0) {
            this.conditionOnlyCfs.add(WritableUtils.readCompressedByteArray(input));
            conditionOnlyCfsSize--;
        }
    }

    @Override
    public void write(DataOutput output) throws IOException {
        WritableUtils.writeCompressedByteArray(output, this.emptyCFName);
        long[] longArrayOfBitSet = trackedColumns.toLongArray();
        WritableUtils.writeVInt(output, longArrayOfBitSet.length);
        for (Long l : longArrayOfBitSet) {
            WritableUtils.writeVLong(output, l);
        }
        WritableUtils.writeVInt(output, encodingScheme.ordinal());
        WritableUtils.writeVInt(output, this.conditionOnlyCfs.size());
        for (byte[] f : this.conditionOnlyCfs) {
            WritableUtils.writeCompressedByteArray(output, f);
        }
    }

    @Override
    public byte[] toByteArray() throws IOException {
        return Writables.getBytes(this);
    }
    
    public static EncodedQualifiersColumnProjectionFilter parseFrom(final byte [] pbBytes) throws DeserializationException {
        try {
            return (EncodedQualifiersColumnProjectionFilter)Writables.getWritable(pbBytes, new EncodedQualifiersColumnProjectionFilter());
        } catch (IOException e) {
            throw new DeserializationException(e);
        }
    }
    
    @Override
    public void filterRowCells(List<Cell> kvs) throws IOException {
        if (kvs.isEmpty()) return;
        Cell firstKV = kvs.get(0);
        Iterables.removeIf(kvs, new Predicate<Cell>() {
            @Override
            public boolean apply(Cell kv) {
                int qualifier = encodingScheme.decode(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength());
                return !trackedColumns.get(qualifier);
            }
        });
        if (kvs.isEmpty()) {
            kvs.add(new KeyValue(firstKV.getRowArray(), firstKV.getRowOffset(), firstKV.getRowLength(),
                    this.emptyCFName, 0, this.emptyCFName.length, ENCODED_EMPTY_COLUMN_BYTES, 0,
                    ENCODED_EMPTY_COLUMN_BYTES.length, HConstants.LATEST_TIMESTAMP, Type.Maximum, null, 0, 0));
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
      return ReturnCode.INCLUDE_AND_NEXT_COL;
    }
    
    interface ColumnTracker {
        
    }
}
