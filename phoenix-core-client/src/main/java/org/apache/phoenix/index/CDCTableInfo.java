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
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.TupleProjectionCompiler;
import org.apache.phoenix.coprocessor.generated.CDCInfoProtos;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.SchemaUtil;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.phoenix.query.QueryConstants.CDC_JSON_COL_NAME;
import static org.apache.phoenix.query.QueryConstants.NAME_SEPARATOR;


/**
 * CDC Table Def Class
 */
public class CDCTableInfo {
    private List<CDCColumnInfo> columnInfoList;
    private byte[] defaultColumnFamily;
    private final Set<PTable.CDCChangeScope> includeScopes;
    private PTable.QualifierEncodingScheme qualifierEncodingScheme;
    private final byte[] cdcJsonColQualBytes;
    private final TupleProjector dataTableProjector;

    private CDCTableInfo(List<CDCColumnInfo> columnInfoList,
                         Set<PTable.CDCChangeScope> includeScopes, byte[] cdcJsonColQualBytes,
                         TupleProjector dataTableProjector) {
        Collections.sort(columnInfoList);
        this.columnInfoList = columnInfoList;
        this.includeScopes = includeScopes;
        this.cdcJsonColQualBytes = cdcJsonColQualBytes;
        this.dataTableProjector = dataTableProjector;
    }

    public CDCTableInfo(byte[] defaultColumnFamily, List<CDCColumnInfo> columnInfoList,
                        Set<PTable.CDCChangeScope> includeScopes,
                        PTable.QualifierEncodingScheme qualifierEncodingScheme,
                        byte[] cdcJsonColQualBytes, TupleProjector dataTableProjector) {
        this(columnInfoList, includeScopes, cdcJsonColQualBytes, dataTableProjector);
        this.defaultColumnFamily = defaultColumnFamily;
        this.qualifierEncodingScheme = qualifierEncodingScheme;
    }

    public List<CDCColumnInfo> getColumnInfoList() {
        return columnInfoList;
    }

    public byte[] getDefaultColumnFamily() {
        return defaultColumnFamily;
    }

    public PTable.QualifierEncodingScheme getQualifierEncodingScheme() {
        return qualifierEncodingScheme;
    }

    public Set<PTable.CDCChangeScope> getIncludeScopes() {
        return includeScopes;
    }

    public byte[] getCdcJsonColQualBytes() {
        return cdcJsonColQualBytes;
    }

    public TupleProjector getDataTableProjector() {
        return dataTableProjector;
    }

    public static CDCTableInfo createFromProto(CDCInfoProtos.CDCTableDef table) {
        byte[] defaultColumnFamily = QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES;
        if (table.hasDefaultFamilyName()) {
            defaultColumnFamily = table.getDefaultFamilyName().toByteArray();
        }
        // For backward compatibility. Clients older than 4.10 will always have
        // non-encoded qualifiers.
        PTable.QualifierEncodingScheme qualifierEncodingScheme
                = PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS;
        if (table.hasQualifierEncodingScheme()) {
            qualifierEncodingScheme = PTable.QualifierEncodingScheme.fromSerializedValue(
                    table.getQualifierEncodingScheme().toByteArray()[0]);
        }
        List<CDCColumnInfo> columns = Lists.newArrayListWithExpectedSize(table.getColumnsCount());
        for (CDCInfoProtos.CDCColumnDef curColumnProto : table.getColumnsList()) {
            columns.add(CDCColumnInfo.createFromProto(curColumnProto));
        }
        String includeScopesStr = table.getCdcIncludeScopes();
        Set<PTable.CDCChangeScope> changeScopeSet;
        try {
            changeScopeSet = CDCUtil.makeChangeScopeEnumsFromString(includeScopesStr);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        TupleProjector dataTableProjector = null;
        if (table.hasDataTableProjectorBytes()) {
            dataTableProjector = TupleProjector.deserializeProjectorFromBytes(
                    table.getDataTableProjectorBytes().toByteArray());
        }
        return new CDCTableInfo(defaultColumnFamily, columns, changeScopeSet,
                qualifierEncodingScheme, table.getCdcJsonColQualBytes().toByteArray(),
                dataTableProjector);
    }

    public static CDCInfoProtos.CDCTableDef toProto(StatementContext context)
            throws SQLException {
        PTable cdcTable = context.getCDCTableRef().getTable();
        PTable dataTable = context.getCDCDataTableRef().getTable();
        CDCInfoProtos.CDCTableDef.Builder builder = CDCInfoProtos.CDCTableDef.newBuilder();
        if (dataTable.getDefaultFamilyName() != null) {
            builder.setDefaultFamilyName(
                    ByteStringer.wrap(dataTable.getDefaultFamilyName().getBytes()));
        }
        String cdcIncludeScopes = context.getEncodedCdcIncludeScopes();
        if (cdcIncludeScopes != null) {
            builder.setCdcIncludeScopes(cdcIncludeScopes);
        }
        if (dataTable.getEncodingScheme() != null) {
            builder.setQualifierEncodingScheme(ByteStringer.wrap(
                    new byte[] { dataTable.getEncodingScheme().getSerializedMetadataValue() }));
        }
        for (PColumn column : dataTable.getColumns()) {
            if (column.getFamilyName() == null) {
                continue;
            }
            builder.addColumns(CDCColumnInfo.toProto(column));
        }
        PColumn cdcJsonCol = cdcTable.getColumnForColumnName(CDC_JSON_COL_NAME);
        builder.setCdcJsonColQualBytes(ByteStringer.wrap(cdcJsonCol.getColumnQualifierBytes()));

        TableRef cdcDataTableRef = context.getCDCDataTableRef();
        if (cdcDataTableRef.getTable().isImmutableRows() &&
                cdcDataTableRef.getTable().getImmutableStorageScheme() ==
                        PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS) {
            List<ColumnRef> dataColumns = new ArrayList<ColumnRef>();
            PTable table = cdcDataTableRef.getTable();
            for (PColumn column : table.getColumns()) {
                if (!SchemaUtil.isPKColumn(column)) {
                    dataColumns.add(new ColumnRef(cdcDataTableRef, column.getPosition()));
                }
            }

            PTable projectedDataTable = TupleProjectionCompiler.createProjectedTable(
                    cdcDataTableRef, dataColumns, false);;
            TupleProjector dataTableProjector = new TupleProjector(projectedDataTable);
            builder.setDataTableProjectorBytes(ByteStringer.wrap(
                    TupleProjector.serializeProjectorIntoBytes(dataTableProjector)));
        }

        return builder.build();
    }

    /**
     * CDC Column Def Class
     */
    public static class CDCColumnInfo implements Comparable<CDCColumnInfo> {

        private final byte[] columnFamily;
        private final byte[] columnQualifier;
        private final String columnName;
        private final PDataType columnType;
        private final String columnFamilyName;
        private String columnDisplayName;

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

        public String getColumnDisplayName(CDCTableInfo tableInfo) {
            if (columnDisplayName == null) {
                // Don't include Column Family if it is a default column Family
                if (Arrays.equals(getColumnFamily(), tableInfo.getDefaultColumnFamily())) {
                    columnDisplayName = getColumnName();
                } else {
                    columnDisplayName = getColumnFamilyName()
                            + NAME_SEPARATOR + getColumnName();
                }
            }
            return columnDisplayName;
        }
    }
}
