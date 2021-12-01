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
package org.apache.phoenix.hbase.index;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;

public abstract class AbstractValueGetter implements ValueGetter{
    @Override
    public KeyValue getLatestKeyValue(ColumnReference ref, long ts) throws IOException {
        ImmutableBytesWritable value = getLatestValue(ref, ts);
        byte[] rowKey = getRowKey();
        int valueOffset = 0;
        int valueLength = 0;
        byte[] valueBytes = HConstants.EMPTY_BYTE_ARRAY;
        if (value == null) {
            return null;
        } else {
            valueBytes = value.get();
            valueOffset = value.getOffset();
            valueLength = value.getLength();
        }
        return new KeyValue(rowKey, 0, rowKey.length, ref.getFamily(), 0, ref.getFamily().length,
                ref.getQualifier(), 0, ref.getQualifier().length, ts, KeyValue.Type.Put,
                valueBytes, valueOffset, valueLength);
    }
 }
