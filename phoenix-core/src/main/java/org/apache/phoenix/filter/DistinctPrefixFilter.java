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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Writable;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.ByteUtil;

public class DistinctPrefixFilter extends FilterBase implements Writable {
    private static byte VERSION = 1;

    private int offset;
    private RowKeySchema schema;
    private int prefixLengh;
    private boolean filterAll = false;
    private final ImmutableBytesWritable lastKey = new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY, -1, -1);

    public DistinctPrefixFilter() {
    }

    public DistinctPrefixFilter(RowKeySchema schema, int prefixLength) {
        this.schema = schema;
        this.prefixLengh = prefixLength;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();

        // First determine the prefix based on the schema
        int maxOffset = schema.iterator(v.getRowArray(), v.getRowOffset()+offset, v.getRowLength()-offset, ptr);
        schema.next(ptr, 0, maxOffset, prefixLengh - 1);

        // now check whether we have seen this prefix before
        if (lastKey.getLength() != ptr.getLength() || !Bytes.equals(ptr.get(), ptr.getOffset(),
                ptr.getLength(), lastKey.get(), lastKey.getOffset(), ptr.getLength())) {
            // if we haven't seen this prefix, include the row and remember this prefix
            lastKey.set(ptr.get(), ptr.getOffset(), ptr.getLength());
            return ReturnCode.INCLUDE;
        }
        // we've seen this prefix already, seek to the next
        return ReturnCode.SEEK_NEXT_USING_HINT;
    }

    @Override
    public Cell getNextCellHint(Cell v) throws IOException {
        PDataType<?> type = schema.getField(prefixLengh-1).getDataType();

        ImmutableBytesWritable tmp;
        // In the following we make sure we copy the key at most once
        // Either because we have an offset, or when needed for nextKey
        if (offset > 0) {
            // make space to copy the missing offset, also 0-pad here if needed
            // (since we're making a copy anyway)
            byte[] tmpKey = new byte[offset + lastKey.getLength() + 
                                     (reversed || type.isFixedWidth() ? 0 : 1)];
            System.arraycopy(v.getRowArray(), v.getRowOffset(), tmpKey, 0, offset);
            System.arraycopy(lastKey.get(), lastKey.getOffset(), tmpKey, offset, lastKey.getLength());
            tmp = new ImmutableBytesWritable(tmpKey);
            if (!reversed) {
                // calculate the next key, the above already 0-padded if needed
                if (!ByteUtil.nextKey(tmp.get(), tmp.getOffset(), tmp.getLength())) {
                    filterAll = true;
                }
            }
        } else {
            if (reversed) {
                // simply seek right before the first occurrence of the row
                tmp = lastKey;
            } else {
                if (type.isFixedWidth()) {
                    // copy the bytes, since nextKey will modify in place
                    tmp = new ImmutableBytesWritable(lastKey.copyBytes());
                } else {
                    // pad with a 0x00 byte (makes a copy)
                    tmp = new ImmutableBytesWritable(lastKey);
                    ByteUtil.nullPad(tmp, tmp.getLength() + 1);
                }
                // calculate the next key
                if (!ByteUtil.nextKey(tmp.get(), tmp.getOffset(), tmp.getLength())) {
                    filterAll = true;
                }
            }
        }
        return KeyValue.createFirstOnRow(tmp.get(), tmp.getOffset(), tmp.getLength(), null, 0, 0,
                null, 0, 0);
    }

    @Override
    public boolean filterAllRemaining() throws IOException {
      return filterAll;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(VERSION);
        schema.write(out);
        out.writeInt(prefixLengh);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        in.readByte(); // ignore
        schema = new RowKeySchema();
        schema.readFields(in);
        prefixLengh = in.readInt();
    }

    @Override
    public byte[] toByteArray() throws IOException {
        return Writables.getBytes(this);
    }

    public static DistinctPrefixFilter parseFrom(final byte[] pbBytes)
            throws DeserializationException {
        try {
            return (DistinctPrefixFilter) Writables.getWritable(pbBytes,
                    new DistinctPrefixFilter());
        } catch (IOException e) {
            throw new DeserializationException(e);
        }
    }
}
