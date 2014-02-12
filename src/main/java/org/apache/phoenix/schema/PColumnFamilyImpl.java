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
package org.apache.phoenix.schema;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.*;

public class PColumnFamilyImpl implements PColumnFamily {
    private final PName name;
    private final List<PColumn> columns;
    private final Map<String, PColumn> columnByString;
    private final Map<byte[], PColumn> columnByBytes;
    
    public PColumnFamilyImpl(PName name, List<PColumn> columns) {
        this.name = name;
        this.columns = ImmutableList.copyOf(columns);
        ImmutableMap.Builder<String, PColumn> columnByStringBuilder = ImmutableMap.builder();
        ImmutableSortedMap.Builder<byte[], PColumn> columnByBytesBuilder = ImmutableSortedMap.orderedBy(Bytes.BYTES_COMPARATOR);
        for (PColumn column : columns) {
            columnByBytesBuilder.put(column.getName().getBytes(), column);
            columnByStringBuilder.put(column.getName().getString(), column);
        }
        this.columnByBytes = columnByBytesBuilder.build();
        this.columnByString = columnByStringBuilder.build();
    }
    
    @Override
    public PName getName() {
        return name;
    }

    @Override
    public List<PColumn> getColumns() {
        return columns;
    }

    @Override
    public PColumn getColumn(byte[] qualifier) throws ColumnNotFoundException  {
        PColumn column = columnByBytes.get(qualifier);
        if (column == null) {
            throw new ColumnNotFoundException(Bytes.toString(qualifier));
        }
        return column;
    }
    
    @Override
    public PColumn getColumn(String name) throws ColumnNotFoundException  {
        PColumn column = columnByString.get(name);
        if (column == null) {
            throw new ColumnNotFoundException(name);
        }
        return column;
    }
}
