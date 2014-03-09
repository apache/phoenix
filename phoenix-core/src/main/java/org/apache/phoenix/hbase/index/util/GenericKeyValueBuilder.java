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
package org.apache.phoenix.hbase.index.util;

import static org.apache.phoenix.hbase.index.util.ImmutableBytesPtr.copyBytesIfNecessary;

import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * {@link KeyValueBuilder} that does simple byte[] copies to build the underlying key-value. This is exactly the same
 * behavior as currently used in {@link Delete} and {@link Put}.
 */
public class GenericKeyValueBuilder extends KeyValueBuilder {

    public static final KeyValueBuilder INSTANCE = new GenericKeyValueBuilder();

    private GenericKeyValueBuilder() {
        // private ctor for singleton
    }

    @Override
    public KeyValue buildPut(ImmutableBytesWritable row, ImmutableBytesWritable family,
            ImmutableBytesWritable qualifier, long ts, ImmutableBytesWritable value) {
        return build(row, family, qualifier, ts, Type.Put, value);
    }

    @Override
    public KeyValue buildDeleteFamily(ImmutableBytesWritable row, ImmutableBytesWritable family,
            ImmutableBytesWritable qualifier, long ts) {
        return build(row, family, qualifier, ts, Type.DeleteFamily, null);
    }

    @Override
    public KeyValue buildDeleteColumns(ImmutableBytesWritable row, ImmutableBytesWritable family,
            ImmutableBytesWritable qualifier, long ts) {
        return build(row, family, qualifier, ts, Type.DeleteColumn, null);
    }

    @Override
    public KeyValue buildDeleteColumn(ImmutableBytesWritable row, ImmutableBytesWritable family,
            ImmutableBytesWritable qualifier, long ts) {
        return build(row, family, qualifier, ts, Type.Delete, null);
    }

    private KeyValue build(ImmutableBytesWritable row, ImmutableBytesWritable family, ImmutableBytesWritable qualifier,
            long ts, KeyValue.Type type, ImmutableBytesWritable value) {
        return new KeyValue(copyBytesIfNecessary(row), copyBytesIfNecessary(family), copyBytesIfNecessary(qualifier),
                ts, type, value == null ? null : copyBytesIfNecessary(value));
    }

    @Override
    public int compareQualifier(Cell kv, byte[] key, int offset, int length) {
        return Bytes.compareTo(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength(), key, offset, length);
    }

    @Override
    public int compareFamily(Cell kv, byte[] key, int offset, int length) {
        return Bytes.compareTo(kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength(), key, offset, length);
    }

    @Override
    public void getValueAsPtr(Cell kv, ImmutableBytesWritable writable) {
        writable.set(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength());
    }

    @Override
    public KVComparator getKeyValueComparator() {
        return KeyValue.COMPARATOR;
    }

    @Override
    public int compareRow(Cell kv, byte[] rrow, int roffset, int rlength) {
        return Bytes.compareTo(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), rrow, roffset, rlength);
    }

    @Override
    public List<Mutation> cloneIfNecessary(List<Mutation> mutations) {
        return mutations;
    }
}