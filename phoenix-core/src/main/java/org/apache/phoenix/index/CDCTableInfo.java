package org.apache.phoenix.index;

import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.phoenix.coprocessor.generated.CDCInfoProtos;
import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.util.CDCUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CDCTableInfo {
    private List<CDCColumnInfo> columnInfoList;
    private byte[] defaultColumnFamily;
    public CDCTableInfo(byte[] defaultColumnFamily, List<CDCColumnInfo> columnInfoList) {
        Collections.sort(columnInfoList);
        this.columnInfoList = columnInfoList;
        this.defaultColumnFamily = defaultColumnFamily;
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

    public void setDefaultColumnFamily(byte[] defaultColumnFamily) {
        this.defaultColumnFamily = defaultColumnFamily;
    }

    public static CDCTableInfo createFromProto(CDCInfoProtos.CDCTableDef table) {
        byte[] defaultColumnFamily = QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES;
        if (table.hasDefaultFamilyName()) {
            defaultColumnFamily = table.getDefaultFamilyName().toByteArray();
        }
        List<CDCColumnInfo> columns = Lists.newArrayListWithExpectedSize(table.getColumnsCount());
        for (CDCInfoProtos.CDCColumnDef curColumnProto : table.getColumnsList()) {
            columns.add(CDCColumnInfo.createFromProto(curColumnProto));
        }
        return new CDCTableInfo(defaultColumnFamily, columns);
    }

    public static CDCInfoProtos.CDCTableDef toProto(PTable table) {
        CDCInfoProtos.CDCTableDef.Builder builder = CDCInfoProtos.CDCTableDef.newBuilder();
        if (table.getDefaultFamilyName() != null) {
            builder.setDefaultFamilyName(ByteStringer.wrap(table.getDefaultFamilyName().getBytes()));
        }
        for (PColumn column : table.getColumns()) {
            if (column.getFamilyName() == null) {
                continue;
            }
            builder.addColumns(CDCColumnInfo.toProto(column));
        }
        return builder.build();
    }

    public static class CDCColumnInfo implements Comparable<CDCColumnInfo>{

        private byte[] columnFamily;
        private byte[] columnQualifier;
        private String columnName;
        private PDataType columnType;

        public CDCColumnInfo(byte[] columnFamily, byte[] columnQualifier, String columnName, PDataType columnType) {
            this.columnFamily = columnFamily;
            this.columnQualifier = columnQualifier;
            this.columnName = columnName;
            this.columnType = columnType;
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
            return new CDCColumnInfo(familyNameBytes, columnQualifierBytes, columnName, dataType);
        }

        public static CDCInfoProtos.CDCColumnDef toProto(PColumn column) {
            CDCInfoProtos.CDCColumnDef.Builder builder = CDCInfoProtos.CDCColumnDef.newBuilder();
            builder.setColumnName(column.getName().toString());
            if (column.getFamilyName() != null) {
                builder.setFamilyNameBytes(ByteStringer.wrap(column.getFamilyName().getBytes()));
            }
            if (column.getDataType()!=null) {
                builder.setDataType(column.getDataType().getSqlTypeName());
            }
            if (column.getColumnQualifierBytes() != null) {
                builder.setColumnQualifierBytes(ByteStringer.wrap(column.getColumnQualifierBytes()));
            }
            return builder.build();
        }
    }
}
