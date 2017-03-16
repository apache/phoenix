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

import static org.apache.phoenix.util.EncodedColumnsUtil.usesEncodedColumnNames;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.SizedUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;

public class PColumnFamilyImpl implements PColumnFamily {
    private final PName name;
    private final List<PColumn> columns;
    private final Map<String, PColumn> columnNamesByStrings;
    private final Map<byte[], PColumn> columnNamesByBytes;
    private final Map<byte[], PColumn> columnsByQualifiers;
    private final int estimatedSize;

    @Override
    public int getEstimatedSize() {
        return estimatedSize;
    }
    
    public PColumnFamilyImpl(PName name, List<PColumn> columns) {
        Preconditions.checkNotNull(name);
        // Include guidePosts also in estimating the size
        long estimatedSize = SizedUtil.OBJECT_SIZE + SizedUtil.POINTER_SIZE * 5 + SizedUtil.INT_SIZE + name.getEstimatedSize() +
                SizedUtil.sizeOfMap(columns.size()) * 2 + SizedUtil.sizeOfArrayList(columns.size());
        this.name = name;
        this.columns = ImmutableList.copyOf(columns);
        ImmutableMap.Builder<String, PColumn> columnNamesByStringBuilder = ImmutableMap.builder();
        ImmutableSortedMap.Builder<byte[], PColumn> columnNamesByBytesBuilder = ImmutableSortedMap.orderedBy(Bytes.BYTES_COMPARATOR);
        ImmutableSortedMap.Builder<byte[], PColumn> columnsByQualifiersBuilder = ImmutableSortedMap.orderedBy(Bytes.BYTES_COMPARATOR);
        for (PColumn column : columns) {
            estimatedSize += column.getEstimatedSize();
            columnNamesByBytesBuilder.put(column.getName().getBytes(), column);
            columnNamesByStringBuilder.put(column.getName().getString(), column);
            // In certain cases like JOIN, PK columns are assigned a column family. So they
            // are not evaluated as a PK column. However, their column qualifier bytes are
            // still null.
            if (!SchemaUtil.isPKColumn(column) && column.getColumnQualifierBytes() != null) {
                columnsByQualifiersBuilder.put(column.getColumnQualifierBytes(), column);
            }
        }
        this.columnNamesByBytes = columnNamesByBytesBuilder.build();
        this.columnNamesByStrings = columnNamesByStringBuilder.build();
        this.columnsByQualifiers =  columnsByQualifiersBuilder.build();
        this.estimatedSize = (int)estimatedSize;
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
    public PColumn getPColumnForColumnNameBytes(byte[] columnNameBytes) throws ColumnNotFoundException  {
        PColumn column = columnNamesByBytes.get(columnNameBytes);
        if (column == null) {
            throw new ColumnNotFoundException(Bytes.toString(columnNameBytes));
        }
        return column;
    }
    
    @Override
    public PColumn getPColumnForColumnName(String columnName) throws ColumnNotFoundException  {
        PColumn column = columnNamesByStrings.get(columnName);
        if (column == null) {
            throw new ColumnNotFoundException(columnName);
        }
        return column;
    }
    
    
    //TODO: samarth think about backward compatibility here
    @Override
    public PColumn getPColumnForColumnQualifier(byte[] cq) throws ColumnNotFoundException {
        Preconditions.checkNotNull(cq);
        return columnsByQualifiers.get(cq);
    }
}
