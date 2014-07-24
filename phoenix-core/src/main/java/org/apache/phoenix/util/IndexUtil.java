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
package org.apache.phoenix.util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.visitor.RowKeyExpressionVisitor;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.join.TupleProjector;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;

import com.google.common.collect.Lists;

public class IndexUtil {
    public static final String INDEX_COLUMN_NAME_SEP = ":";
    public static final byte[] INDEX_COLUMN_NAME_SEP_BYTES = Bytes.toBytes(INDEX_COLUMN_NAME_SEP);
    
    private IndexUtil() {
    }

    // Since we cannot have nullable fixed length in a row key
    // we need to translate to variable length.
    public static PDataType getIndexColumnDataType(PColumn dataColumn) throws SQLException {
        PDataType type = getIndexColumnDataType(dataColumn.isNullable(),dataColumn.getDataType());
        if (type == null) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_INDEX_COLUMN_ON_TYPE).setColumnName(dataColumn.getName().getString())
            .setMessage("Type="+dataColumn.getDataType()).build().buildException();
        }
        return type;
    }
    
    // Since we cannot have nullable fixed length in a row key
    // we need to translate to variable length. The verification that we have a valid index
    // row key was already done, so here we just need to covert from one built-in type to
    // another.
    public static PDataType getIndexColumnDataType(boolean isNullable, PDataType dataType) {
        if (dataType == null || !isNullable || !dataType.isFixedWidth()) {
            return dataType;
        }
        // for fixed length numeric types and boolean
        if (dataType.isCastableTo(PDataType.DECIMAL)) {
            return PDataType.DECIMAL;
        }
        // for CHAR
        if (dataType.isCoercibleTo(PDataType.VARCHAR)) {
            return PDataType.VARCHAR;
        }
        throw new IllegalArgumentException("Unsupported non nullable index type " + dataType);
    }
    

    public static String getDataColumnName(String name) {
        return name.substring(name.indexOf(INDEX_COLUMN_NAME_SEP) + 1);
    }

    public static String getDataColumnFamilyName(String name) {
        return name.substring(0,name.indexOf(INDEX_COLUMN_NAME_SEP));
    }

    public static String getDataColumnFullName(String name) {
        int index = name.indexOf(INDEX_COLUMN_NAME_SEP) ;
        if (index == 0) {
            return name.substring(index+1);
        }
        return SchemaUtil.getColumnDisplayName(name.substring(0, index), name.substring(index+1));
    }

    public static String getIndexColumnName(String dataColumnFamilyName, String dataColumnName) {
        return (dataColumnFamilyName == null ? "" : dataColumnFamilyName) + INDEX_COLUMN_NAME_SEP + dataColumnName;
    }
    
    public static byte[] getIndexColumnName(byte[] dataColumnFamilyName, byte[] dataColumnName) {
        return ByteUtil.concat(dataColumnFamilyName == null ?  ByteUtil.EMPTY_BYTE_ARRAY : dataColumnFamilyName, INDEX_COLUMN_NAME_SEP_BYTES, dataColumnName);
    }
    
    public static String getIndexColumnName(PColumn dataColumn) {
        String dataColumnFamilyName = SchemaUtil.isPKColumn(dataColumn) ? null : dataColumn.getFamilyName().getString();
        return getIndexColumnName(dataColumnFamilyName, dataColumn.getName().getString());
    }

    public static PColumn getDataColumn(PTable dataTable, String indexColumnName) {
        int pos = indexColumnName.indexOf(INDEX_COLUMN_NAME_SEP);
        if (pos < 0) {
            throw new IllegalArgumentException("Could not find expected '" + INDEX_COLUMN_NAME_SEP +  "' separator in index column name of \"" + indexColumnName + "\"");
        }
        if (pos == 0) {
            try {
                return dataTable.getPKColumn(indexColumnName.substring(1));
            } catch (ColumnNotFoundException e) {
                throw new IllegalArgumentException("Could not find PK column \"" +  indexColumnName.substring(pos+1) + "\" in index column name of \"" + indexColumnName + "\"", e);
            }
        }
        PColumnFamily family;
        try {
            family = dataTable.getColumnFamily(indexColumnName.substring(0, pos));
        } catch (ColumnFamilyNotFoundException e) {
            throw new IllegalArgumentException("Could not find column family \"" +  indexColumnName.substring(0, pos) + "\" in index column name of \"" + indexColumnName + "\"", e);
        }
        try {
            return family.getColumn(indexColumnName.substring(pos+1));
        } catch (ColumnNotFoundException e) {
            throw new IllegalArgumentException("Could not find column \"" +  indexColumnName.substring(pos+1) + "\" in index column name of \"" + indexColumnName + "\"", e);
        }
    }

    private static boolean isEmptyKeyValue(PTable table, ColumnReference ref) {
        byte[] emptyKeyValueCF = SchemaUtil.getEmptyColumnFamily(table);
        return (Bytes.compareTo(emptyKeyValueCF, ref.getFamily()) == 0 &&
                Bytes.compareTo(QueryConstants.EMPTY_COLUMN_BYTES, ref.getQualifier()) == 0);
    }

    public static List<Mutation> generateIndexData(final PTable table, PTable index,
            List<Mutation> dataMutations, ImmutableBytesWritable ptr, final KeyValueBuilder kvBuilder)
            throws SQLException {
        try {
            IndexMaintainer maintainer = index.getIndexMaintainer(table);
            List<Mutation> indexMutations = Lists.newArrayListWithExpectedSize(dataMutations.size());
           for (final Mutation dataMutation : dataMutations) {
                long ts = MetaDataUtil.getClientTimeStamp(dataMutation);
                ptr.set(dataMutation.getRow());
                if (dataMutation instanceof Put) {
                    // TODO: is this more efficient than looking in our mutation map
                    // using the key plus finding the PColumn?
                    ValueGetter valueGetter = new ValueGetter() {
        
                        @Override
                        public ImmutableBytesPtr getLatestValue(ColumnReference ref) {
                            // Always return null for our empty key value, as this will cause the index
                            // maintainer to always treat this Put as a new row.
                            if (isEmptyKeyValue(table, ref)) {
                                return null;
                            }
                            Map<byte [], List<Cell>> familyMap = dataMutation.getFamilyCellMap();
                            byte[] family = ref.getFamily();
                            List<Cell> kvs = familyMap.get(family);
                            if (kvs == null) {
                                return null;
                            }
                            byte[] qualifier = ref.getQualifier();
                            for (Cell kv : kvs) {
                                if (Bytes.compareTo(kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength(), family, 0, family.length) == 0 &&
                                    Bytes.compareTo(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength(), qualifier, 0, qualifier.length) == 0) {
                                  ImmutableBytesPtr ptr = new ImmutableBytesPtr();
                                  kvBuilder.getValueAsPtr(kv, ptr);
                                  return ptr;
                                }
                            }
                            return null;
                        }
                        
                    };
                    indexMutations.add(maintainer.buildUpdateMutation(kvBuilder, valueGetter, ptr, ts, null, null));
                } else {
                    // We can only generate the correct Delete if we have no KV columns in our index.
                    // Perhaps it'd be best to ignore Delete mutations all together here, as this
                    // gets triggered typically for an initial population where Delete markers make
                    // little sense.
                    if (maintainer.getIndexedColumns().isEmpty()) {
                        indexMutations.add(maintainer.buildDeleteMutation(kvBuilder, ptr, ts));
                    }
                }
            }
            return indexMutations;
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    public static boolean isDataPKColumn(PColumn column) {
        return column.getName().getString().startsWith(INDEX_COLUMN_NAME_SEP);
    }
    
    public static boolean getViewConstantValue(PColumn column, ImmutableBytesWritable ptr) {
        byte[] value = column.getViewConstant();
        if (value != null) {
            ptr.set(value, 0, value.length-1);
            return true;
        }
        return false;
    }
    
    /**
     * Traverse the expression tree and set the offset of every RowKeyColumnExpression
     * to the offset provided. This is used for local indexing on the server-side to
     * skip over the region start key that prefixes index rows.
     * @param rootExpression the root expression from which to begin traversal
     * @param offset the offset to set on each RowKeyColumnExpression
     */
    public static void setRowKeyExpressionOffset(Expression rootExpression, final int offset) {
        rootExpression.accept(new RowKeyExpressionVisitor() {

            @Override
            public Void visit(RowKeyColumnExpression node) {
                node.setOffset(offset);
                return null;
            }
            
        });
    }
    
    public static HRegion getIndexRegion(RegionCoprocessorEnvironment environment) throws IOException {
        HRegion userRegion = environment.getRegion();
        TableName indexTableName = TableName.valueOf(MetaDataUtil.getLocalIndexPhysicalName(userRegion.getTableDesc().getName()));
        List<HRegion> onlineRegions = environment.getRegionServerServices().getOnlineRegions(indexTableName);
        for(HRegion indexRegion : onlineRegions) {
            if (Bytes.compareTo(userRegion.getStartKey(), indexRegion.getStartKey()) == 0) {
                return indexRegion;
            }
        }
        return null;
    }

    public static HRegion getDataRegion(RegionCoprocessorEnvironment env) throws IOException {
        HRegion indexRegion = env.getRegion();
        TableName dataTableName = TableName.valueOf(MetaDataUtil.getUserTableName(indexRegion.getTableDesc().getNameAsString()));
        List<HRegion> onlineRegions = env.getRegionServerServices().getOnlineRegions(dataTableName);
        for(HRegion region : onlineRegions) {
            if (Bytes.compareTo(indexRegion.getStartKey(), region.getStartKey()) == 0) {
                return region;
            }
        }
        return null;
    }

    public static ColumnReference[] deserializeDataTableColumnsToJoin(Scan scan) {
        byte[] columnsBytes = scan.getAttribute(BaseScannerRegionObserver.DATA_TABLE_COLUMNS_TO_JOIN);
        if (columnsBytes == null) return null;
        ByteArrayInputStream stream = new ByteArrayInputStream(columnsBytes); // TODO: size?
        try {
            DataInputStream input = new DataInputStream(stream);
            int numColumns = WritableUtils.readVInt(input);
            ColumnReference[] dataColumns = new ColumnReference[numColumns];
            for (int i = 0; i < numColumns; i++) {
                dataColumns[i] = new ColumnReference(Bytes.readByteArray(input), Bytes.readByteArray(input));
            }
            return dataColumns;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static byte[][] deserializeViewConstantsFromScan(Scan scan) {
        byte[] bytes = scan.getAttribute(BaseScannerRegionObserver.VIEW_CONSTANTS);
        if (bytes == null) return null;
        ByteArrayInputStream stream = new ByteArrayInputStream(bytes); // TODO: size?
        try {
            DataInputStream input = new DataInputStream(stream);
            int numConstants = WritableUtils.readVInt(input);
            byte[][] viewConstants = new byte[numConstants][];
            for (int i = 0; i < numConstants; i++) {
                viewConstants[i] = Bytes.readByteArray(input);
            }
            return viewConstants;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    public static KeyValueSchema deserializeLocalIndexJoinSchemaFromScan(final Scan scan) {
        byte[] schemaBytes = scan.getAttribute(BaseScannerRegionObserver.LOCAL_INDEX_JOIN_SCHEMA);
        if (schemaBytes == null) return null;
        ByteArrayInputStream stream = new ByteArrayInputStream(schemaBytes); // TODO: size?
        try {
            DataInputStream input = new DataInputStream(stream);
            KeyValueSchema schema = new KeyValueSchema();
            schema.readFields(input);
            return schema;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    public static TupleProjector getTupleProjector(Scan scan, ColumnReference[] dataColumns) {
        if (dataColumns != null && dataColumns.length != 0) {
            KeyValueSchema keyValueSchema = deserializeLocalIndexJoinSchemaFromScan(scan); 
            KeyValueColumnExpression[] keyValueColumns = new KeyValueColumnExpression[dataColumns.length];
            for (int i = 0; i < dataColumns.length; i++) {
                ColumnReference dataColumn = dataColumns[i];
                KeyValueColumnExpression dataColumnExpr = new KeyValueColumnExpression(keyValueSchema.getField(i), dataColumn.getFamily(), dataColumn.getQualifier());
                keyValueColumns[i] = dataColumnExpr;
            }
            return new TupleProjector(keyValueSchema, keyValueColumns);
        }
        return null;
    }
    
    public static void wrapResultUsingOffset(List<Cell> result, final int offset,
            ColumnReference[] dataColumns, TupleProjector tupleProjector, HRegion dataRegion,
            IndexMaintainer indexMaintainer, byte[][] viewConstants, ImmutableBytesWritable ptr) throws IOException {
        if (tupleProjector != null) {
            // Join back to data table here by issuing a local get projecting
            // all of the cq:cf from the KeyValueColumnExpression into the Get.
            Cell firstCell = result.get(0);
            byte[] indexRowKey = firstCell.getRowArray();
            ptr.set(indexRowKey, firstCell.getRowOffset() + offset, firstCell.getRowLength() - offset);
            byte[] dataRowKey = indexMaintainer.buildDataRowKey(ptr, viewConstants);
            Get get = new Get(dataRowKey);
            for (int i = 0; i < dataColumns.length; i++) {
                get.addColumn(dataColumns[i].getFamily(), dataColumns[i].getQualifier());
            }
            Result joinResult = dataRegion.get(get);
            // TODO: handle null case (but shouldn't happen)
            Tuple joinTuple = new ResultTuple(joinResult);
            // This will create a byte[] that captures all of the values from the data table
            byte[] value =
                    tupleProjector.getSchema().toBytes(joinTuple, tupleProjector.getExpressions(),
                        tupleProjector.getValueBitSet(), ptr);
            KeyValue keyValue =
                    KeyValueUtil.newKeyValue(firstCell.getRowArray(),firstCell.getRowOffset(),firstCell.getRowLength(), TupleProjector.VALUE_COLUMN_FAMILY,
                        TupleProjector.VALUE_COLUMN_QUALIFIER, firstCell.getTimestamp(), value, 0, value.length);
            result.add(keyValue);
        }
        for (int i = 0; i < result.size(); i++) {
            final Cell cell = result.get(i);
            // TODO: Create DelegateCell class instead
            Cell newCell = new Cell() {

                @Override
                public byte[] getRowArray() {
                    return cell.getRowArray();
                }

                @Override
                public int getRowOffset() {
                    return cell.getRowOffset() + offset;
                }

                @Override
                public short getRowLength() {
                    return (short)(cell.getRowLength() - offset);
                }

                @Override
                public byte[] getFamilyArray() {
                    return cell.getFamilyArray();
                }

                @Override
                public int getFamilyOffset() {
                    return cell.getFamilyOffset();
                }

                @Override
                public byte getFamilyLength() {
                    return cell.getFamilyLength();
                }

                @Override
                public byte[] getQualifierArray() {
                    return cell.getQualifierArray();
                }

                @Override
                public int getQualifierOffset() {
                    return cell.getQualifierOffset();
                }

                @Override
                public int getQualifierLength() {
                    return cell.getQualifierLength();
                }

                @Override
                public long getTimestamp() {
                    return cell.getTimestamp();
                }

                @Override
                public byte getTypeByte() {
                    return cell.getTypeByte();
                }

                @Override
                public long getMvccVersion() {
                    return cell.getMvccVersion();
                }

                @Override
                public byte[] getValueArray() {
                    return cell.getValueArray();
                }

                @Override
                public int getValueOffset() {
                    return cell.getValueOffset();
                }

                @Override
                public int getValueLength() {
                    return cell.getValueLength();
                }

                @Override
                public byte[] getTagsArray() {
                    return cell.getTagsArray();
                }

                @Override
                public int getTagsOffset() {
                    return cell.getTagsOffset();
                }

                @Override
                public short getTagsLength() {
                    return cell.getTagsLength();
                }

                @Override
                public byte[] getValue() {
                    return cell.getValue();
                }

                @Override
                public byte[] getFamily() {
                    return cell.getFamily();
                }

                @Override
                public byte[] getQualifier() {
                    return cell.getQualifier();
                }

                @Override
                public byte[] getRow() {
                    return cell.getRow();
                }

                @Override
                @Deprecated
                public int getTagsLengthUnsigned() {
                    return cell.getTagsLengthUnsigned();
                }
            };
            // Wrap cell in cell that offsets row key
            result.set(i, newCell);
        }
    }
}
