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

import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.LOCAL_INDEX_BUILD;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.LOCAL_INDEX_BUILD_PROTO;
import static org.apache.phoenix.coprocessor.MetaDataProtocol.PHOENIX_MAJOR_VERSION;
import static org.apache.phoenix.coprocessor.MetaDataProtocol.PHOENIX_MINOR_VERSION;
import static org.apache.phoenix.coprocessor.MetaDataProtocol.PHOENIX_PATCH_NUMBER;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES;
import static org.apache.phoenix.query.QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX;
import static org.apache.phoenix.query.QueryConstants.VALUE_COLUMN_FAMILY;
import static org.apache.phoenix.query.QueryConstants.VALUE_COLUMN_QUALIFIER;
import static org.apache.phoenix.util.PhoenixRuntime.getTable;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.PhoenixTagType;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.RawCell;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.cache.Cache;
import org.apache.phoenix.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.compat.hbase.OffsetCell;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.IndexStatementRewriter;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.WhereCompiler;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MutationCode;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataResponse;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataService;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.UpdateIndexStateRequest;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.MutationState.MultiRowMutationState;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.SingleCellColumnExpression;
import org.apache.phoenix.expression.visitor.RowKeyExpressionVisitor;
import org.apache.phoenix.hbase.index.AbstractValueGetter;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.index.GlobalIndexChecker;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.protobuf.ProtobufUtil;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.ValueSchema.Field;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.transaction.PhoenixTransactionProvider.Feature;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

public class IndexUtil {
    public static final String INDEX_COLUMN_NAME_SEP = ":";
    public static final byte[] INDEX_COLUMN_NAME_SEP_BYTES = Bytes.toBytes(INDEX_COLUMN_NAME_SEP);

    private final static Cache<String, Boolean> indexNameGlobalIndexCheckerEnabledMap =
        CacheBuilder.newBuilder()
            .expireAfterWrite(QueryServicesOptions.GLOBAL_INDEX_CHECKER_ENABLED_MAP_EXPIRATION_MIN,
                    TimeUnit.MINUTES)
            .build();

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
    // row key was already done, so here we just need to convert from one built-in type to
    // another.
    public static PDataType getIndexColumnDataType(boolean isNullable, PDataType dataType) {
        if (dataType == null || !isNullable || !dataType.isFixedWidth()) {
            return dataType;
        }
        // for fixed length numeric types and boolean
        if (dataType.isCastableTo(PDecimal.INSTANCE)) {
            return PDecimal.INSTANCE;
        }
        // for CHAR
        if (dataType.isCoercibleTo(PVarchar.INSTANCE)) {
            return PVarchar.INSTANCE;
        }

        if (PBinary.INSTANCE.equals(dataType)) {
            return PVarbinary.INSTANCE;
        }
        throw new IllegalArgumentException("Unsupported non nullable type " + dataType);
    }
    

    public static String getDataColumnName(String name) {
        return name.substring(name.indexOf(INDEX_COLUMN_NAME_SEP) + 1);
    }

    public static String getDataColumnFamilyName(String name) {
        return name.substring(0,name.indexOf(INDEX_COLUMN_NAME_SEP));
    }

    public static String getActualColumnFamilyName(String name) {
        if(name.startsWith(LOCAL_INDEX_COLUMN_FAMILY_PREFIX)) {
            return name.substring(LOCAL_INDEX_COLUMN_FAMILY_PREFIX.length());
        }
        return name;
    }

    public static String getCaseSensitiveDataColumnFullName(String name) {
        int index = name.indexOf(INDEX_COLUMN_NAME_SEP) ;
        return SchemaUtil.getCaseSensitiveColumnDisplayName(getDataColumnFamilyName(name), name.substring(index+1));
    }

    public static String getIndexColumnName(String dataColumnFamilyName, String dataColumnName) {
        return (dataColumnFamilyName == null ? "" : dataColumnFamilyName) + INDEX_COLUMN_NAME_SEP
                + dataColumnName;
    }
    
    public static byte[] getIndexColumnName(byte[] dataColumnFamilyName, byte[] dataColumnName) {
        return ByteUtil.concat(dataColumnFamilyName == null ?  ByteUtil.EMPTY_BYTE_ARRAY : dataColumnFamilyName, INDEX_COLUMN_NAME_SEP_BYTES, dataColumnName);
    }

    public static String getIndexColumnName(PColumn dataColumn) {
        String dataColumnFamilyName = SchemaUtil.isPKColumn(dataColumn) ? null : dataColumn.getFamilyName().getString();
        return getIndexColumnName(dataColumnFamilyName, dataColumn.getName().getString());
    }
    
	public static PColumn getIndexPKColumn(int position, PColumn dataColumn) {
		assert (SchemaUtil.isPKColumn(dataColumn));
		PName indexColumnName = PNameFactory.newName(getIndexColumnName(null, dataColumn.getName().getString()));
		PColumn column = new PColumnImpl(indexColumnName, null, dataColumn.getDataType(), dataColumn.getMaxLength(),
				dataColumn.getScale(), dataColumn.isNullable(), position, dataColumn.getSortOrder(),
				dataColumn.getArraySize(), null, false, dataColumn.getExpressionStr(), dataColumn.isRowTimestamp(), false,
				// TODO set the columnQualifierBytes correctly
				/*columnQualifierBytes*/null, HConstants.LATEST_TIMESTAMP); 
		return column;
	}

    public static String getLocalIndexColumnFamily(String dataColumnFamilyName) {
        return dataColumnFamilyName == null ? null
                : QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX + dataColumnFamilyName;
    }
    
    public static byte[] getLocalIndexColumnFamily(byte[] dataColumnFamilyBytes) {
        String dataCF = Bytes.toString(dataColumnFamilyBytes);
        return getLocalIndexColumnFamily(dataCF).getBytes(StandardCharsets.UTF_8);
    }
    
    public static PColumn getDataColumn(PTable dataTable, String indexColumnName) {
        PColumn column = getDataColumnOrNull(dataTable, indexColumnName);
        if (column == null) {
            throw new IllegalArgumentException("Could not find column \"" + SchemaUtil.getColumnName(getDataColumnFamilyName(indexColumnName), getDataColumnName(indexColumnName)) + " in " + dataTable);
        }
        return column;
    }
    
    public static PColumn getDataColumnOrNull(PTable dataTable, String indexColumnName) {
        int pos = indexColumnName.indexOf(INDEX_COLUMN_NAME_SEP);
        if (pos < 0) {
            return null;
        }
        if (pos == 0) {
            try {
                return dataTable.getPKColumn(indexColumnName.substring(1));
            } catch (ColumnNotFoundException e) {
                return null;
            }
        }
        PColumnFamily family;
        try {
            family = dataTable.getColumnFamily(getDataColumnFamilyName(indexColumnName));                
        } catch (ColumnFamilyNotFoundException e) {
            return null;
        }
        try {
            return family.getPColumnForColumnName(indexColumnName.substring(pos+1));
        } catch (ColumnNotFoundException e) {
            return null;
        }
    }
    
    /**
     * Return a list of {@code PColumn} for the associated data columns given the corresponding index columns. For a tenant
     * specific view, the connection needs to be tenant specific too. 
     * @param dataTableName
     * @param indexColumns
     * @param conn
     * @return
     * @throws TableNotFoundException if table cannot be found in the connection's metdata cache
     */
    public static List<PColumn> getDataColumns(String dataTableName, List<PColumn> indexColumns, PhoenixConnection conn) throws SQLException {
        PTable dataTable =  getTable(conn, dataTableName);
        List<PColumn> dataColumns = new ArrayList<PColumn>(indexColumns.size());
        for (PColumn indexColumn : indexColumns) {
            dataColumns.add(getDataColumn(dataTable, indexColumn.getName().getString()));
        }
        return dataColumns;
    }
    

    private static boolean isEmptyKeyValue(PTable table, ColumnReference ref) {
        byte[] emptyKeyValueCF = SchemaUtil.getEmptyColumnFamily(table);
        byte[] emptyKeyValueQualifier = EncodedColumnsUtil.getEmptyKeyValueInfo(table).getFirst();
        return (Bytes.compareTo(emptyKeyValueCF, 0, emptyKeyValueCF.length, ref.getFamilyWritable()
                .get(), ref.getFamilyWritable().getOffset(), ref.getFamilyWritable().getLength()) == 0 && Bytes
                .compareTo(emptyKeyValueQualifier, 0,
                        emptyKeyValueQualifier.length, ref.getQualifierWritable().get(), ref
                            .getQualifierWritable().getOffset(), ref.getQualifierWritable()
                            .getLength()) == 0);
    }


    public static boolean isGlobalIndexCheckerEnabled(PhoenixConnection connection, PName index)
            throws SQLException {
        String indexName = index.getString();
        Boolean entry = indexNameGlobalIndexCheckerEnabledMap.getIfPresent(indexName);
        if (entry != null){
            return entry;
        }

        boolean result = false;
        try {
            TableDescriptor desc = connection.getQueryServices().getTableDescriptor(index.getBytes());

            if (desc != null) {
                if (desc.hasCoprocessor(GlobalIndexChecker.class.getName())) {
                    result = true;
                }
            }
            indexNameGlobalIndexCheckerEnabledMap.put(indexName, result);
        } catch (TableNotFoundException ex) {
            // We can swallow this because some indexes don't have separate tables like local indexes
        }

        return result;
    }

    public static List<Mutation> generateIndexData(final PTable table, PTable index,
            final MultiRowMutationState multiRowMutationState, List<Mutation> dataMutations, final KeyValueBuilder kvBuilder, PhoenixConnection connection)
            throws SQLException {
        try {
            final ImmutableBytesPtr ptr = new ImmutableBytesPtr();
            IndexMaintainer maintainer = index.getIndexMaintainer(table, connection);
            List<Mutation> indexMutations = Lists.newArrayListWithExpectedSize(dataMutations.size());
            for (final Mutation dataMutation : dataMutations) {
                long ts = MetaDataUtil.getClientTimeStamp(dataMutation);
                ptr.set(dataMutation.getRow());
                /*
                 * We only need to generate the additional mutations for a Put for immutable indexes.
                 * Deletes of rows are handled by running a re-written query against the index table,
                 * and Deletes of column values should never be necessary, as you should never be
                 * updating an existing row.
                 */
                if (dataMutation instanceof Put) {
                    ValueGetter valueGetter = new AbstractValueGetter() {

                        @Override
                        public byte[] getRowKey() {
                            return dataMutation.getRow();
                        }

                        @Override
                        public ImmutableBytesWritable getLatestValue(ColumnReference ref, long ts) {
                            // Always return null for our empty key value, as this will cause the index
                            // maintainer to always treat this Put as a new row.
                            if (isEmptyKeyValue(table, ref)) {
                                return null;
                            }
                            byte[] family = ref.getFamily();
                            byte[] qualifier = ref.getQualifier();
                            Map<byte [], List<Cell>> familyMap = dataMutation.getFamilyCellMap();
                            List<Cell> kvs = familyMap.get(family);
                            if (kvs == null) {
                                return null;
                            }
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
                    byte[] regionStartKey = null;
                    byte[] regionEndkey = null;
                    if(maintainer.isLocalIndex()) {
                        HRegionLocation tableRegionLocation = connection.getQueryServices().getTableRegionLocation(table.getPhysicalName().getBytes(), dataMutation.getRow());
                        regionStartKey = tableRegionLocation.getRegion().getStartKey();
                        regionEndkey = tableRegionLocation.getRegion().getEndKey();
                    }
                    indexMutations.add(maintainer.buildUpdateMutation(kvBuilder, valueGetter, ptr, ts, regionStartKey, regionEndkey, false));
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
    
    public static boolean isIndexColumn(String name) {
        return name.contains(INDEX_COLUMN_NAME_SEP);
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

    public static List<IndexMaintainer> deSerializeIndexMaintainersFromScan(Scan scan) {
        boolean useProto = false;
        byte[] indexBytes = scan.getAttribute(LOCAL_INDEX_BUILD_PROTO);
        useProto = indexBytes != null;
        if (indexBytes == null) {
            indexBytes = scan.getAttribute(LOCAL_INDEX_BUILD);
        }
        if (indexBytes == null) {
            indexBytes = scan.getAttribute(PhoenixIndexCodec.INDEX_PROTO_MD);
            useProto = indexBytes != null;
        }
        if (indexBytes == null) {
            indexBytes = scan.getAttribute(PhoenixIndexCodec.INDEX_MD);
        }
        List<IndexMaintainer> indexMaintainers =
                indexBytes == null ? null : IndexMaintainer.deserialize(indexBytes, useProto);
        return indexMaintainers;
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
            boolean storeColsInSingleCell = scan.getAttribute(BaseScannerRegionObserver.COLUMNS_STORED_IN_SINGLE_CELL) != null;
            QualifierEncodingScheme encodingScheme = EncodedColumnsUtil.getQualifierEncodingScheme(scan);
            ImmutableStorageScheme immutableStorageScheme = EncodedColumnsUtil.getImmutableStorageScheme(scan);
            Expression[] colExpressions = storeColsInSingleCell ? new SingleCellColumnExpression[dataColumns.length] : new KeyValueColumnExpression[dataColumns.length];
            for (int i = 0; i < dataColumns.length; i++) {
                byte[] family = dataColumns[i].getFamily();
                byte[] qualifier = dataColumns[i].getQualifier();
                Field field = keyValueSchema.getField(i);
                Expression dataColumnExpr =
                        storeColsInSingleCell ? new SingleCellColumnExpression(field, family, qualifier, encodingScheme, immutableStorageScheme)
                            : new KeyValueColumnExpression(field, family, qualifier);
                colExpressions[i] = dataColumnExpr;
            }
            return new TupleProjector(keyValueSchema, colExpressions);
        }
        return null;
    }
    
    /**
     * Rewrite a view statement to be valid against an index
     * @param conn
     * @param index
     * @param table
     * @return
     * @throws SQLException
     */
    public static String rewriteViewStatement(PhoenixConnection conn, PTable index, PTable table, String viewStatement) throws SQLException {
        if (viewStatement == null) {
            return null;
        }
        SelectStatement select = new SQLParser(viewStatement).parseQuery();
        ColumnResolver resolver = FromCompiler.getResolver(new TableRef(table));
        SelectStatement translatedSelect = IndexStatementRewriter.translate(select, resolver);
        ParseNode whereNode = translatedSelect.getWhere();
        PhoenixStatement statement = new PhoenixStatement(conn);
        TableRef indexTableRef = new TableRef(index) {
            @Override
            public String getColumnDisplayName(ColumnRef ref, boolean schemaNameCaseSensitive, boolean colNameCaseSensitive) {
                return '"' + ref.getColumn().getName().getString() + '"';
            }
        };
        ColumnResolver indexResolver = FromCompiler.getResolver(indexTableRef);
        StatementContext context = new StatementContext(statement, indexResolver);
        // Compile to ensure validity
        WhereCompiler.compile(context, whereNode);
        StringBuilder buf = new StringBuilder();
        whereNode.toSQL(indexResolver, buf);
        return QueryUtil.getViewStatement(index.getSchemaName().getString(), index.getTableName().getString(), buf.toString());
    }

    public static void addTupleAsOneCell(List<Cell> result,
                                 Tuple tuple,
                                 TupleProjector tupleProjector,
                                 ImmutableBytesWritable ptr) {
        // This will create a byte[] that captures all of the values from the data table
        byte[] value =
                tupleProjector.getSchema().toBytes(tuple, tupleProjector.getExpressions(),
                        tupleProjector.getValueBitSet(), ptr);
        Cell firstCell = result.get(0);
        Cell keyValue =
                PhoenixKeyValueUtil.newKeyValue(firstCell.getRowArray(),
                        firstCell.getRowOffset(),firstCell.getRowLength(), VALUE_COLUMN_FAMILY,
                        VALUE_COLUMN_QUALIFIER, firstCell.getTimestamp(), value, 0, value.length);
        result.add(keyValue);
    }

    public static void wrapResultUsingOffset(List<Cell> result, final int offset) throws IOException {
        ListIterator<Cell> itr = result.listIterator();
        while (itr.hasNext()) {
            final Cell cell = itr.next();
            // TODO: Create DelegateCell class instead
            Cell newCell = new OffsetCell(cell, offset);
            itr.set(newCell);
        }
    }

    public static String getIndexColumnExpressionStr(PColumn col) {
        return col.getExpressionStr() == null ? IndexUtil.getCaseSensitiveDataColumnFullName(col.getName().getString())
                : col.getExpressionStr();
    }

    public static byte[][] getViewConstants(PTable dataTable) {
        if (dataTable.getType() != PTableType.VIEW && dataTable.getType() != PTableType.PROJECTED) return null;
        int dataPosOffset = (dataTable.getBucketNum() != null ? 1 : 0) + (dataTable.isMultiTenant() ? 1 : 0);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        List<byte[]> viewConstants = new ArrayList<byte[]>();
        List<PColumn> dataPkColumns = dataTable.getPKColumns();
        for (int i = dataPosOffset; i < dataPkColumns.size(); i++) {
            PColumn dataPKColumn = dataPkColumns.get(i);
            if (dataPKColumn.getViewConstant() != null) {
                if (IndexUtil.getViewConstantValue(dataPKColumn, ptr)) {
                    viewConstants.add(ByteUtil.copyKeyBytesIfNecessary(ptr));
                } else {
                    throw new IllegalStateException();
                }
            }
        }
        return viewConstants.isEmpty() ? null : viewConstants
                .toArray(new byte[viewConstants.size()][]);
    }

    public static void writeLocalUpdates(Region region, final List<Mutation> mutations, boolean skipWAL) throws IOException {
        if(skipWAL) {
            for (Mutation m : mutations) {
                m.setDurability(Durability.SKIP_WAL);
            }
        }
        region.batchMutate(
            mutations.toArray(new Mutation[mutations.size()]));
    }
    
    public static MetaDataMutationResult updateIndexState(String indexTableName, long minTimeStamp,
            Table metaTable, PIndexState newState) throws Throwable {
        byte[] indexTableKey = SchemaUtil.getTableKeyFromFullName(indexTableName);
        return updateIndexState(indexTableKey, minTimeStamp, metaTable, newState);
    }
    
    public static MetaDataMutationResult updateIndexState(byte[] indexTableKey, long minTimeStamp,
            Table metaTable, PIndexState newState) throws Throwable {
        // Mimic the Put that gets generated by the client on an update of the index state
        Put put = new Put(indexTableKey);
        put.addColumn(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES, PhoenixDatabaseMetaData.INDEX_STATE_BYTES,
                newState.getSerializedBytes());
        put.addColumn(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES, PhoenixDatabaseMetaData.INDEX_DISABLE_TIMESTAMP_BYTES,
                PLong.INSTANCE.toBytes(minTimeStamp));
        put.addColumn(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES, PhoenixDatabaseMetaData.ASYNC_REBUILD_TIMESTAMP_BYTES,
                PLong.INSTANCE.toBytes(0));
        final List<Mutation> tableMetadata = Collections.<Mutation> singletonList(put);

        final Map<byte[], MetaDataResponse> results = metaTable.coprocessorService(MetaDataService.class, indexTableKey,
                indexTableKey, new Batch.Call<MetaDataService, MetaDataResponse>() {
                    @Override
                    public MetaDataResponse call(MetaDataService instance) throws IOException {
                        ServerRpcController controller = new ServerRpcController();
                        BlockingRpcCallback<MetaDataResponse> rpcCallback = new BlockingRpcCallback<>();
                        UpdateIndexStateRequest.Builder builder = UpdateIndexStateRequest.newBuilder();
                        for (Mutation m : tableMetadata) {
                            MutationProto mp = ProtobufUtil.toProto(m);
                            builder.addTableMetadataMutations(mp.toByteString());
                        }
                        builder.setClientVersion(VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION, PHOENIX_PATCH_NUMBER));
                        instance.updateIndexState(controller, builder.build(), rpcCallback);
                        if (controller.getFailedOn() != null) { throw controller.getFailedOn(); }
                        return rpcCallback.get();
                    }
                });
        if (results.isEmpty()) { throw new IOException("Didn't get expected result size"); }
        MetaDataResponse tmpResponse = results.values().iterator().next();
        return MetaDataMutationResult.constructFromProto(tmpResponse);
    }

    public static boolean matchingSplitKeys(byte[][] splitKeys1, byte[][] splitKeys2) throws IOException {
        if (splitKeys1 != null && splitKeys2 != null && splitKeys1.length == splitKeys2.length) {
            for (int i = 0; i < splitKeys1.length; i++) {
                if (Bytes.compareTo(splitKeys1[i], splitKeys2[i]) != 0) { return false; }
            }
        } else {
            return false;
        }
        return true;
    }

    public static boolean isLocalIndexStore(Store store) {
        return store.getColumnFamilyDescriptor().getNameAsString().startsWith(QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX);
    }
    
    public static PTable getPDataTable(Connection conn, TableDescriptor tableDesc) throws SQLException {
        String dataTableName = Bytes.toString(tableDesc.getValue(MetaDataUtil.DATA_TABLE_NAME_PROP_BYTES));
        String physicalTableName = tableDesc.getTableName().getNameAsString();
        PTable pDataTable = null;
        if (dataTableName == null) {
            if (physicalTableName.contains(QueryConstants.NAMESPACE_SEPARATOR)) {
                try {
                    pDataTable = PhoenixRuntime.getTable(conn, physicalTableName
                            .replace(QueryConstants.NAMESPACE_SEPARATOR, QueryConstants.NAME_SEPARATOR));
                } catch (TableNotFoundException e) {
                    // could be a table mapped to external table
                    pDataTable = PhoenixRuntime.getTable(conn, physicalTableName);
                }
            }else{
                pDataTable = PhoenixRuntime.getTable(conn, physicalTableName);
            }
        } else {
            pDataTable = PhoenixRuntime.getTable(conn, dataTableName);
        }
        return pDataTable;
    }
    
    public static boolean isLocalIndexFamily(String family) {
        return family.indexOf(LOCAL_INDEX_COLUMN_FAMILY_PREFIX) != -1;
    }

    public static void updateIndexState(PhoenixConnection conn, String indexTableName,
            PIndexState newState, Long indexDisableTimestamp) throws SQLException {
        updateIndexState(conn, indexTableName, newState, indexDisableTimestamp, HConstants.LATEST_TIMESTAMP);
    }
    
    public static void updateIndexState(PhoenixConnection conn, String indexTableName,
    		PIndexState newState, Long indexDisableTimestamp, Long expectedMaxTimestamp) throws SQLException {
    	byte[] indexTableKey = SchemaUtil.getTableKeyFromFullName(indexTableName);
    	String schemaName = SchemaUtil.getSchemaNameFromFullName(indexTableName);
    	String indexName = SchemaUtil.getTableNameFromFullName(indexTableName);
    	// Mimic the Put that gets generated by the client on an update of the
    	// index state
    	Put put = new Put(indexTableKey);
    	put.addColumn(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES, PhoenixDatabaseMetaData.INDEX_STATE_BYTES,
                expectedMaxTimestamp,
    			newState.getSerializedBytes());
        if (indexDisableTimestamp != null) {
            put.addColumn(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                PhoenixDatabaseMetaData.INDEX_DISABLE_TIMESTAMP_BYTES,
                expectedMaxTimestamp,
                PLong.INSTANCE.toBytes(indexDisableTimestamp));
        }
        if (newState == PIndexState.ACTIVE) {
            put.addColumn(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                PhoenixDatabaseMetaData.ASYNC_REBUILD_TIMESTAMP_BYTES, PLong.INSTANCE.toBytes(0));
        }
    	final List<Mutation> tableMetadata = Collections.<Mutation> singletonList(put);
    	MetaDataMutationResult result = conn.getQueryServices().updateIndexState(tableMetadata, null);
    	MutationCode code = result.getMutationCode();
    	if (code == MutationCode.TABLE_NOT_FOUND) {
    		throw new TableNotFoundException(schemaName, indexName);
    	}
    	if (code == MutationCode.UNALLOWED_TABLE_MUTATION) {
    		throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_INDEX_STATE_TRANSITION)
    				.setMessage("indexState=" + newState).setSchemaName(schemaName)
    				.setTableName(indexName).build().buildException();
    	}
    }

    public static List<PTable> getClientMaintainedIndexes(PTable table) {
        Iterator<PTable> indexIterator = // Only maintain tables with immutable rows through this client-side mechanism
                (table.isTransactional() && table.getTransactionProvider().getTransactionProvider().isUnsupported(Feature.MAINTAIN_LOCAL_INDEX_ON_SERVER)) ?
                         IndexMaintainer.maintainedIndexes(table.getIndexes().iterator()) :
                             (table.isImmutableRows() || table.isTransactional()) ?
                                 // If the data table has a different storage scheme than index table, don't maintain this on the client
                                 // For example, if the index is single cell but the data table is one_cell, if there is a partial update on the data table, index can't be built on the client.
                                IndexMaintainer.maintainedGlobalIndexesWithMatchingStorageScheme(table, table.getIndexes().iterator()) :
                                    Collections.<PTable>emptyIterator();
        return Lists.newArrayList(indexIterator);
    }

    public static Result incrementCounterForIndex(PhoenixConnection conn, String failedIndexTable,long amount) throws IOException {
        byte[] indexTableKey = SchemaUtil.getTableKeyFromFullName(failedIndexTable);
        Increment incr = new Increment(indexTableKey);
        incr.addColumn(TABLE_FAMILY_BYTES, PhoenixDatabaseMetaData.PENDING_DISABLE_COUNT_BYTES, amount);
        try (Table table = conn.getQueryServices().getTable(
                SchemaUtil.getPhysicalTableName(
                    PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME,
                    conn.getQueryServices().getProps()).getName())) {
            return table.increment(incr);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    public static long getIndexPendingDisableCount(PhoenixConnection conn, String failedIndexTable) throws IOException {
        byte[] indexTableKey = SchemaUtil.getTableKeyFromFullName(failedIndexTable);
        Get get = new Get(indexTableKey);
        get.addColumn(TABLE_FAMILY_BYTES, PhoenixDatabaseMetaData.PENDING_DISABLE_COUNT_BYTES);
        try (Table table = conn.getQueryServices().getTable(
                SchemaUtil.getPhysicalTableName(
                    PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME,
                    conn.getQueryServices().getProps()).getName())) {
            Result result = table.get(get);
            return Bytes.toLong(result.getValue(TABLE_FAMILY_BYTES, PhoenixDatabaseMetaData.PENDING_DISABLE_COUNT_BYTES));
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    public static long getIndexPendingDisableCountLastUpdatedTimestamp(
            PhoenixConnection conn, String failedIndexTable)
            throws IOException {
        byte[] indexTableKey =
            SchemaUtil.getTableKeyFromFullName(failedIndexTable);
        Get get = new Get(indexTableKey);
        get.addColumn(TABLE_FAMILY_BYTES,
            PhoenixDatabaseMetaData.PENDING_DISABLE_COUNT_BYTES);
        byte[] systemCatalog = SchemaUtil.getPhysicalTableName(
            PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME,
            conn.getQueryServices().getProps()).getName();
        try (Table table = conn.getQueryServices().getTable(systemCatalog)) {
            Result result = table.get(get);
            Cell cell = result.listCells().get(0);
            return cell.getTimestamp();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    /**
     * Set Cell Tags to delete markers with source of operation attribute.
     * @param miniBatchOp miniBatchOp
     * @throws IOException IOException
     */
    public static void setDeleteAttributes(
            MiniBatchOperationInProgress<Mutation> miniBatchOp)
            throws IOException {
        for (int i = 0; i < miniBatchOp.size(); i++) {
            Mutation m = miniBatchOp.getOperation(i);
            if (!(m instanceof Delete)) {
                // Ignore if it is not Delete type.
                continue;
            }
            byte[] sourceOpAttr =
                    m.getAttribute(QueryServices.SOURCE_OPERATION_ATTRIB);
            if (sourceOpAttr == null) {
                continue;
            }
            Tag sourceOpTag = new ArrayBackedTag(
                    PhoenixTagType.SOURCE_OPERATION_TAG_TYPE, sourceOpAttr);
            List<Cell> updatedCells = new ArrayList<>();
            for (CellScanner cellScanner = m.cellScanner();
                 cellScanner.advance();) {
                Cell cell = cellScanner.current();
                RawCell rawCell = (RawCell) cell;
                List<Tag> tags = new ArrayList<>();
                Iterator<Tag> tagsIterator = rawCell.getTags();
                while (tagsIterator.hasNext()) {
                    tags.add(tagsIterator.next());
                }
                tags.add(sourceOpTag);
                // TODO: PrivateCellUtil's IA is Private.
                // HBASE-25328 adds a builder methodfor creating Tag which
                // will be LP with IA.coproc
                Cell updatedCell = PrivateCellUtil.createCell(cell, tags);
                updatedCells.add(updatedCell);
            }
            m.getFamilyCellMap().clear();
            // Clear and add new Cells to the Mutation.
            for (Cell cell : updatedCells) {
                Delete d = (Delete) m;
                d.addDeleteMarker(cell);
            }
        }
    }

    public static boolean isHintedGlobalIndex(final TableRef tableRef) {
        PTable table = tableRef.getTable();
        return table.getType() == PTableType.INDEX
                && table.getIndexType() == PTable.IndexType.GLOBAL
                && tableRef.isHinted();
    }

    /**
     * Updates the EMPTY cell value to VERIFIED for global index table rows.
     */
    public static class IndexStatusUpdater {

        private final byte[] emptyKeyValueCF;
        private final int emptyKeyValueCFLength;
        private final byte[] emptyKeyValueQualifier;
        private final int emptyKeyValueQualifierLength;

        public IndexStatusUpdater(final byte[] emptyKeyValueCF, final byte[] emptyKeyValueQualifier) {
            this.emptyKeyValueCF = emptyKeyValueCF;
            this.emptyKeyValueQualifier = emptyKeyValueQualifier;
            this.emptyKeyValueCFLength = emptyKeyValueCF.length;
            this.emptyKeyValueQualifierLength = emptyKeyValueQualifier.length;
        }

        /**
         * Update the Empty cell values to VERIFIED in the passed keyValues list
         *
         * @param keyValues will be modified
         */
        public void setVerified(List<Cell> keyValues) {
            for (int i = 0; i < keyValues.size(); i++) {
                updateVerified(keyValues.get(i));
            }
        }

        /**
         * Update the Empty cell values to VERIFIED in the passed keyValues list
         *
         * @param cellScanner contents will be modified
         * @throws IOException
         */
        public void setVerified(CellScanner cellScanner) throws IOException {
            while (cellScanner.advance()) {
                updateVerified(cellScanner.current());
            }
        }

        private void updateVerified(Cell cell) {
            if (CellUtil.compareFamilies(cell, emptyKeyValueCF, 0, emptyKeyValueCFLength) == 0
                    && CellUtil.compareQualifiers(cell, emptyKeyValueQualifier,
                        0, emptyKeyValueQualifierLength) == 0) {
                if (cell.getValueLength() != 1) {
                    //This should never happen. Fail fast if it does.
                   throw new IllegalArgumentException("Empty cell value length is not 1");
                }
                //We are directly overwriting the value for performance
                cell.getValueArray()[cell.getValueOffset()] = QueryConstants.VERIFIED_BYTE;
            }
        }
    }
}
