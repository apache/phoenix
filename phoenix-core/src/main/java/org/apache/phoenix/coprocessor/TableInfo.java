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
package org.apache.phoenix.coprocessor;

import static org.apache.phoenix.query.QueryConstants.SEPARATOR_BYTE_ARRAY;

import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.util.ByteUtil;

public class TableInfo {
    private byte[] row;
    private final byte[] tenantId;
    private final byte[] schema;
    private final byte[] name;

    public TableInfo(byte[] row, byte[] tenantId, byte[] schema, byte[] name) {
        this.row = row;
        this.tenantId = tenantId;
        this.schema = schema;
        this.name = name;
    }
    
    public TableInfo(byte[] tenantId, byte[] schema, byte[] name) {
        this(null, tenantId, schema, name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableInfo tableInfo = (TableInfo) o;
        return Arrays.equals(name, tableInfo.name);

    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(name);
    }

    public byte[] getRowKeyPrefix() {
        return ByteUtil.concat(tenantId, SEPARATOR_BYTE_ARRAY, schema, SEPARATOR_BYTE_ARRAY, name);
    }

    @Override
    public String toString() {
        return Bytes.toString(getRowKeyPrefix());
    }
    
    public byte[] getRow() {
        return row;
    }

    public byte[] getTenantId() {
        return tenantId;
    }

    public byte[] getSchemaName() {
        return schema;
    }

    public byte[] getTableName() {
        return name;
    }
}
