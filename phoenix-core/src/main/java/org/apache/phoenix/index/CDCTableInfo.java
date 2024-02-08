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

package org.apache.phoenix.index;

import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.phoenix.coprocessor.generated.CDCInfoProtos;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.util.CDCUtil;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;


/**
 * CDC Table Def Class
 */
public class CDCTableInfo {
    private List<CDCColumnInfo> columnInfoList;
    private byte[] defaultColumnFamily;
    private String cdcIncludeScopes;
    private PTable.QualifierEncodingScheme qualifierEncodingScheme;
    public CDCTableInfo(byte[] defaultColumnFamily,
                        List<CDCColumnInfo> columnInfoList, String cdcIncludeScopes,
                        PTable.QualifierEncodingScheme qualifierEncodingScheme) {
        Collections.sort(columnInfoList);
        this.columnInfoList = columnInfoList;
        this.defaultColumnFamily = defaultColumnFamily;
        this.cdcIncludeScopes = cdcIncludeScopes;
        this.qualifierEncodingScheme = qualifierEncodingScheme;
    }

    public List<CDCColumnInfo> getColumnInfoList() {
        return columnInfoList;
    }

    public void setColumnInfoList(List<CDCColumnInfo> columnInfoList) {
        this.columnInfoList = columnInfoList;
    }

    public byte[] getDefaultColumnFamily() {
        return defaultColumnFamily;
    }

    public PTable.QualifierEncodingScheme getQualifierEncodingScheme() {
        return qualifierEncodingScheme;
    }

    public void setQualifierEncodingScheme(PTable.QualifierEncodingScheme qualifierEncodingScheme) {
        this.qualifierEncodingScheme = qualifierEncodingScheme;
    }

    public void setDefaultColumnFamily(byte[] defaultColumnFamily) {
        this.defaultColumnFamily = defaultColumnFamily;
    }

    public String getCdcIncludeScopes() {
        return cdcIncludeScopes;
    }

    public void setCdcIncludeScopes(String cdcIncludeScopes) {
        this.cdcIncludeScopes = cdcIncludeScopes;
    }

    public static CDCTableInfo createFromProto(CDCInfoProtos.CDCTableDef table) {
        byte[] defaultColumnFamily = QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES;
        if (table.hasDefaultFamilyName()) {
            defaultColumnFamily = table.getDefaultFamilyName().toByteArray();
        }
        // For backward compatibility. Clients older than 4.10 will always have non-encoded qualifiers.
        PTable.QualifierEncodingScheme qualifierEncodingScheme = PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS;
        if (table.hasQualifierEncodingScheme()) {
            qualifierEncodingScheme = PTable.QualifierEncodingScheme.fromSerializedValue(table.getQualifierEncodingScheme().toByteArray()[0]);
        }
        List<CDCColumnInfo> columns = Lists.newArrayListWithExpectedSize(table.getColumnsCount());
        for (CDCInfoProtos.CDCColumnDef curColumnProto : table.getColumnsList()) {
            columns.add(CDCColumnInfo.createFromProto(curColumnProto));
        }
        return new CDCTableInfo(defaultColumnFamily, columns,
                table.getCdcIncludeScopes(), qualifierEncodingScheme);
    }

    public static CDCInfoProtos.CDCTableDef toProto(PTable table, String cdcIncludeScopes) {
        CDCInfoProtos.CDCTableDef.Builder builder = CDCInfoProtos.CDCTableDef.newBuilder();
        if (table.getDefaultFamilyName() != null) {
            builder.setDefaultFamilyName(
                    ByteStringer.wrap(table.getDefaultFamilyName().getBytes()));
        }
        if (cdcIncludeScopes != null) {
            builder.setCdcIncludeScopes(cdcIncludeScopes);
        }
        if (table.getEncodingScheme() != null) {
            builder.setQualifierEncodingScheme(ByteStringer
                    .wrap(new byte[] { table.getEncodingScheme().getSerializedMetadataValue() }));
        }
        for (PColumn column : table.getColumns()) {
            if (column.getFamilyName() == null) {
                continue;
            }
            builder.addColumns(CDCColumnInfo.toProto(column));
        }
        return builder.build();
    }

    /**
     * CDC Column Def Class
     */
    public static class CDCColumnInfo implements Comparable<CDCColumnInfo> {

        private byte[] columnFamily;
        private byte[] columnQualifier;
        private String columnName;
        private PDataType columnType;
        private String columnFamilyName;

        public CDCColumnInfo(byte[] columnFamily, byte[] columnQualifier,
                             String columnName, PDataType columnType,
                             String columnFamilyName) {
            this.columnFamily = columnFamily;
            this.columnQualifier = columnQualifier;
            this.columnName = columnName;
            this.columnType = columnType;
            this.columnFamilyName = columnFamilyName;
        }

        public byte[] getColumnFamily() {
            return columnFamily;
        }

        public void setColumnFamily(byte[] columnFamily) {
            this.columnFamily = columnFamily;
        }

        public void setColumnQualifier(byte[] columnQualifier) {
            this.columnQualifier = columnQualifier;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public void setColumnType(PDataType columnType) {
            this.columnType = columnType;
        }

        public byte[] getColumnQualifier() {
            return columnQualifier;
        }

        public String getColumnName() {
            return columnName;
        }

        public PDataType getColumnType() {
            return columnType;
        }
        public String getColumnFamilyName() {
            return columnFamilyName;
        }

        public void setColumnFamilyName(String columnFamilyName) {
            this.columnFamilyName = columnFamilyName;
        }

        @Override
        public int compareTo(CDCColumnInfo columnInfo) {
            return CDCUtil.compareCellFamilyAndQualifier(this.getColumnFamily(),
                    this.getColumnQualifier(),
                    columnInfo.getColumnFamily(),
                    columnInfo.getColumnQualifier());
        }

        public static CDCColumnInfo createFromProto(CDCInfoProtos.CDCColumnDef column) {
            String columnName = column.getColumnName();
            byte[] familyNameBytes = column.getFamilyNameBytes().toByteArray();
            PDataType dataType = PDataType.fromSqlTypeName(column.getDataType());
            byte[] columnQualifierBytes = column.getColumnQualifierBytes().toByteArray();
            String columnFamilyName = StandardCharsets.UTF_8
                    .decode(ByteBuffer.wrap(familyNameBytes)).toString();
            return new CDCColumnInfo(familyNameBytes,
                    columnQualifierBytes, columnName, dataType, columnFamilyName);
        }

        public static CDCInfoProtos.CDCColumnDef toProto(PColumn column) {
            CDCInfoProtos.CDCColumnDef.Builder builder = CDCInfoProtos.CDCColumnDef.newBuilder();
            builder.setColumnName(column.getName().toString());
            if (column.getFamilyName() != null) {
                builder.setFamilyNameBytes(ByteStringer.wrap(column.getFamilyName().getBytes()));
            }
            if (column.getDataType() != null) {
                builder.setDataType(column.getDataType().getSqlTypeName());
            }
            if (column.getColumnQualifierBytes() != null) {
                builder.setColumnQualifierBytes(
                        ByteStringer.wrap(column.getColumnQualifierBytes()));
            }
            return builder.build();
        }
    }
}
