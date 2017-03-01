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

import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.IndexExpressionCompiler;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos.ColumnInfo;
import org.apache.phoenix.expression.CoerceExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.SingleCellColumnExpression;
import org.apache.phoenix.expression.SingleCellConstructorExpression;
import org.apache.phoenix.expression.visitor.KeyValueExpressionVisitor;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.StatelessTraverseAllParseNodeVisitor;
import org.apache.phoenix.parse.UDFParseNode;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.ValueSchema;
import org.apache.phoenix.schema.ValueSchema.Field;
import org.apache.phoenix.schema.tuple.BaseTuple;
import org.apache.phoenix.schema.tuple.ValueGetterTuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.BitSet;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.ExpressionUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TrustedByteArrayOutputStream;
import org.apache.tephra.TxConstants;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * 
 * Class that builds index row key from data row key and current state of
 * row and caches any covered columns. Client-side serializes into byte array using 
 * @link #serialize(PTable, ImmutableBytesWritable)}
 * and transmits to server-side through either the 
 * {@link org.apache.phoenix.index.PhoenixIndexCodec#INDEX_PROTO_MD}
 * Mutation attribute or as a separate RPC call using 
 * {@link org.apache.phoenix.cache.ServerCacheClient})
 * 
 * 
 * @since 2.1.0
 */
public class IndexMaintainer implements Writable, Iterable<ColumnReference> {

    private static final int EXPRESSION_NOT_PRESENT = -1;
    private static final int ESTIMATED_EXPRESSION_SIZE = 8;
    
    public static IndexMaintainer create(PTable dataTable, PTable index, PhoenixConnection connection) {
        if (dataTable.getType() == PTableType.INDEX || index.getType() != PTableType.INDEX || !dataTable.getIndexes().contains(index)) {
            throw new IllegalArgumentException();
        }
        IndexMaintainer maintainer = new IndexMaintainer(dataTable, index, connection);
        return maintainer;
    }
    
    public static Iterator<PTable> nonDisabledIndexIterator(Iterator<PTable> indexes) {
        return Iterators.filter(indexes, new Predicate<PTable>() {
            @Override
            public boolean apply(PTable index) {
                return !PIndexState.DISABLE.equals(index.getIndexState());
            }
        });
    }
    
    public static Iterator<PTable> enabledGlobalIndexIterator(Iterator<PTable> indexes) {
        return Iterators.filter(indexes, new Predicate<PTable>() {
            @Override
            public boolean apply(PTable index) {
                return !PIndexState.DISABLE.equals(index.getIndexState()) && !index.getIndexType().equals(IndexType.LOCAL);
            }
        });
    }
    
    public static Iterator<PTable> enabledLocalIndexIterator(Iterator<PTable> indexes) {
        return Iterators.filter(indexes, new Predicate<PTable>() {
            @Override
            public boolean apply(PTable index) {
                return !PIndexState.DISABLE.equals(index.getIndexState()) && index.getIndexType().equals(IndexType.LOCAL);
            }
        });
    }
    
    /**
     * For client-side to serialize all IndexMaintainers for a given table
     * @param dataTable data table
     * @param ptr bytes pointer to hold returned serialized value
     */
    public static void serialize(PTable dataTable, ImmutableBytesWritable ptr, PhoenixConnection connection) {
        List<PTable> indexes = dataTable.getIndexes();
        serialize(dataTable, ptr, indexes, connection);
    }

    /**
     * For client-side to serialize all IndexMaintainers for a given table
     * @param dataTable data table
     * @param ptr bytes pointer to hold returned serialized value
     * @param indexes indexes to serialize
     */
    public static void serialize(PTable dataTable, ImmutableBytesWritable ptr,
            List<PTable> indexes, PhoenixConnection connection) {
        Iterator<PTable> indexesItr = nonDisabledIndexIterator(indexes.iterator());
        if ((dataTable.isImmutableRows()) || !indexesItr.hasNext()) {
            indexesItr = enabledLocalIndexIterator(indexesItr);
            if (!indexesItr.hasNext()) {
                ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
                return;
            }
        }
        int nIndexes = 0;
        while (indexesItr.hasNext()) {
            nIndexes++;
            indexesItr.next();
        }
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(stream);
        try {
            // Encode data table salting in sign of number of indexes
            WritableUtils.writeVInt(output, nIndexes * (dataTable.getBucketNum() == null ? 1 : -1));
            // Write out data row key schema once, since it's the same for all index maintainers
            dataTable.getRowKeySchema().write(output);
            indexesItr =
                    dataTable.isImmutableRows() ? enabledLocalIndexIterator(indexes.iterator())
                            : nonDisabledIndexIterator(indexes.iterator());
            while (indexesItr.hasNext()) {
                    org.apache.phoenix.coprocessor.generated.ServerCachingProtos.IndexMaintainer proto = IndexMaintainer.toProto(indexesItr.next().getIndexMaintainer(dataTable, connection));
                    byte[] protoBytes = proto.toByteArray();
                    WritableUtils.writeVInt(output, protoBytes.length);
                    output.write(protoBytes);
            }
        } catch (IOException e) {
            throw new RuntimeException(e); // Impossible
        }
        ptr.set(stream.toByteArray(), 0, stream.size());
    }
    
    /**
     * For client-side to append serialized IndexMaintainers of keyValueIndexes
     * @param dataTable data table
     * @param indexMetaDataPtr bytes pointer to hold returned serialized value
     * @param keyValueIndexes indexes to serialize
     */
    public static void serializeAdditional(PTable table, ImmutableBytesWritable indexMetaDataPtr,
            List<PTable> keyValueIndexes, PhoenixConnection connection) {
        int nMutableIndexes = indexMetaDataPtr.getLength() == 0 ? 0 : ByteUtil.vintFromBytes(indexMetaDataPtr);
        int nIndexes = nMutableIndexes + keyValueIndexes.size();
        int estimatedSize = indexMetaDataPtr.getLength() + 1; // Just in case new size increases buffer
        if (indexMetaDataPtr.getLength() == 0) {
            estimatedSize += table.getRowKeySchema().getEstimatedByteSize();
        }
        for (PTable index : keyValueIndexes) {
            estimatedSize += index.getIndexMaintainer(table, connection).getEstimatedByteSize();
        }
        TrustedByteArrayOutputStream stream = new TrustedByteArrayOutputStream(estimatedSize + 1);
        DataOutput output = new DataOutputStream(stream);
        try {
            // Encode data table salting in sign of number of indexes
            WritableUtils.writeVInt(output, nIndexes * (table.getBucketNum() == null ? 1 : -1));
            // Serialize current mutable indexes, subtracting the vint size from the length
            // as its still included
            if (indexMetaDataPtr.getLength() > 0) {
                output.write(indexMetaDataPtr.get(), indexMetaDataPtr.getOffset(), indexMetaDataPtr.getLength()-WritableUtils.getVIntSize(nMutableIndexes));
            } else {
                table.getRowKeySchema().write(output);
            }
            // Serialize mutable indexes afterwards
            for (PTable index : keyValueIndexes) {
                IndexMaintainer maintainer = index.getIndexMaintainer(table, connection);
                byte[] protoBytes = IndexMaintainer.toProto(maintainer).toByteArray();
                WritableUtils.writeVInt(output, protoBytes.length);
                output.write(protoBytes);
            }
        } catch (IOException e) {
            throw new RuntimeException(e); // Impossible
        }
        indexMetaDataPtr.set(stream.getBuffer(), 0, stream.size());
    }
    
    public static List<IndexMaintainer> deserialize(ImmutableBytesWritable metaDataPtr,
            KeyValueBuilder builder, boolean useProtoForIndexMaintainer) {
        return deserialize(metaDataPtr.get(), metaDataPtr.getOffset(), metaDataPtr.getLength(), useProtoForIndexMaintainer);
    }
    
    public static List<IndexMaintainer> deserialize(byte[] buf, boolean useProtoForIndexMaintainer) {
        return deserialize(buf, 0, buf.length, useProtoForIndexMaintainer);
    }

    private static List<IndexMaintainer> deserialize(byte[] buf, int offset, int length, boolean useProtoForIndexMaintainer) {
        ByteArrayInputStream stream = new ByteArrayInputStream(buf, offset, length);
        DataInput input = new DataInputStream(stream);
        List<IndexMaintainer> maintainers = Collections.emptyList();
        try {
            int size = WritableUtils.readVInt(input);
            boolean isDataTableSalted = size < 0;
            size = Math.abs(size);
            RowKeySchema rowKeySchema = new RowKeySchema();
            rowKeySchema.readFields(input);
            maintainers = Lists.newArrayListWithExpectedSize(size);
            for (int i = 0; i < size; i++) {
                if (useProtoForIndexMaintainer) {
                  int protoSize = WritableUtils.readVInt(input);
                  byte[] b = new byte[protoSize];
                  input.readFully(b);
                  org.apache.phoenix.coprocessor.generated.ServerCachingProtos.IndexMaintainer proto = ServerCachingProtos.IndexMaintainer.parseFrom(b);
                  maintainers.add(IndexMaintainer.fromProto(proto, rowKeySchema, isDataTableSalted));
                } else {
                    IndexMaintainer maintainer = new IndexMaintainer(rowKeySchema, isDataTableSalted);
                    maintainer.readFields(input);
                    maintainers.add(maintainer);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e); // Impossible
        }
        return maintainers;
    }
    
    private byte[] viewIndexId;
    private boolean isMultiTenant;
    // indexed expressions that are not present in the row key of the data table, the expression can also refer to a regular column
    private List<Expression> indexedExpressions;
    // columns required to evaluate all expressions in indexedExpressions (this does not include columns in the data row key)
    private Set<ColumnReference> indexedColumns;
    
    // columns required to create index row i.e. indexedColumns + coveredColumns  (this does not include columns in the data row key)
    private Set<ColumnReference> allColumns;
    // TODO remove this in the next major release
    private List<PDataType> indexedColumnTypes;
    private RowKeyMetaData rowKeyMetaData;
    private byte[] indexTableName;
    private int nIndexSaltBuckets;
    private byte[] dataEmptyKeyValueCF;
    private ImmutableBytesPtr emptyKeyValueCFPtr;
    private int nDataCFs;
    private boolean indexWALDisabled;
    private boolean isLocalIndex;
    private boolean immutableRows;
    // Transient state
    private final boolean isDataTableSalted;
    private final RowKeySchema dataRowKeySchema;
    
    private int estimatedIndexRowKeyBytes;
    private int estimatedExpressionSize;
    private int[] dataPkPosition;
    private int maxTrailingNulls;
    private ColumnReference dataEmptyKeyValueRef;
    private boolean rowKeyOrderOptimizable;
    
    /**** START: New member variables added in 4.10 *****/ 
    private QualifierEncodingScheme encodingScheme;
    private ImmutableStorageScheme immutableStorageScheme;
    /*
     * Information for columns of data tables that are being indexed. The first part of the pair is column family name
     * and second part is the column name. The reason we need to track this state is because for certain storage schemes
     * like ImmutableStorageScheme#SINGLE_CELL_ARRAY_WITH_OFFSETS, the column for which we need to generate an index
     * table put/delete is different from the columns that are indexed in the phoenix schema. This information helps us
     * determine whether or not certain operations like DROP COLUMN should impact the index.
     */
    private Set<Pair<String, String>> indexedColumnsInfo;
    /*
     * Map of covered columns where a key is column reference for a column in the data table
     * and value is column reference for corresponding column in the index table.
     */
    private Map<ColumnReference, ColumnReference> coveredColumnsMap;
    /**** END: New member variables added in 4.10 *****/

    private IndexMaintainer(RowKeySchema dataRowKeySchema, boolean isDataTableSalted) {
        this.dataRowKeySchema = dataRowKeySchema;
        this.isDataTableSalted = isDataTableSalted;
    }
    
    private IndexMaintainer(final PTable dataTable, final PTable index, PhoenixConnection connection) {
        this(dataTable.getRowKeySchema(), dataTable.getBucketNum() != null);
        assert(dataTable.getType() == PTableType.SYSTEM || dataTable.getType() == PTableType.TABLE || dataTable.getType() == PTableType.VIEW);
        this.rowKeyOrderOptimizable = index.rowKeyOrderOptimizable();
        this.isMultiTenant = dataTable.isMultiTenant();
        this.viewIndexId = index.getViewIndexId() == null ? null : MetaDataUtil.getViewIndexIdDataType().toBytes(index.getViewIndexId());
        this.isLocalIndex = index.getIndexType() == IndexType.LOCAL;
        this.encodingScheme = index.getEncodingScheme();
        
        // null check for b/w compatibility
        this.encodingScheme = index.getEncodingScheme() == null ? QualifierEncodingScheme.NON_ENCODED_QUALIFIERS : index.getEncodingScheme();
        this.immutableStorageScheme = index.getImmutableStorageScheme() == null ? ImmutableStorageScheme.ONE_CELL_PER_COLUMN : index.getImmutableStorageScheme();
        
        byte[] indexTableName = index.getPhysicalName().getBytes();
        // Use this for the nDataSaltBuckets as we need this for local indexes
        // TODO: persist nDataSaltBuckets separately, but maintain b/w compat.
        Integer nIndexSaltBuckets = isLocalIndex ? dataTable.getBucketNum() : index.getBucketNum();
        boolean indexWALDisabled = index.isWALDisabled();
        int indexPosOffset = (index.getBucketNum() == null ? 0 : 1) + (this.isMultiTenant ? 1 : 0) + (this.viewIndexId == null ? 0 : 1);
        int nIndexColumns = index.getColumns().size() - indexPosOffset;
        int nIndexPKColumns = index.getPKColumns().size() - indexPosOffset;
        // number of expressions that are indexed that are not present in the row key of the data table
        int indexedExpressionCount = 0;
        for (int i = indexPosOffset; i<index.getPKColumns().size();i++) {
        	PColumn indexColumn = index.getPKColumns().get(i);
        	String indexColumnName = indexColumn.getName().getString();
            String dataFamilyName = IndexUtil.getDataColumnFamilyName(indexColumnName);
            String dataColumnName = IndexUtil.getDataColumnName(indexColumnName);
            try {
                PColumn dataColumn = dataFamilyName.equals("") ? dataTable.getColumnForColumnName(dataColumnName) : dataTable.getColumnFamily(dataFamilyName).getPColumnForColumnName(dataColumnName);
                if (SchemaUtil.isPKColumn(dataColumn)) 
                    continue;
            } catch (ColumnNotFoundException e) {
             // This column must be an expression
            } catch (Exception e) {
                throw new IllegalArgumentException(e);
            }
            indexedExpressionCount++;
        }
        int indexPkColumnCount = this.dataRowKeySchema.getFieldCount() + indexedExpressionCount  - (this.isDataTableSalted ? 1 : 0) - (this.isMultiTenant ? 1 : 0);
        this.rowKeyMetaData = newRowKeyMetaData(indexPkColumnCount);
        BitSet bitSet = this.rowKeyMetaData.getViewConstantColumnBitSet();

        int dataPosOffset = (isDataTableSalted ? 1 : 0) + (this.isMultiTenant ? 1 : 0);
        int nDataPKColumns = dataRowKeySchema.getFieldCount() - dataPosOffset;
        // For indexes on views, we need to remember which data columns are "constants"
        // These are the values in a VIEW where clause. For these, we don't put them in the
        // index, as they're the same for every row in the index.
        if (dataTable.getType() == PTableType.VIEW) {
            List<PColumn>dataPKColumns = dataTable.getPKColumns();
            for (int i = dataPosOffset; i < dataPKColumns.size(); i++) {
                PColumn dataPKColumn = dataPKColumns.get(i);
                if (dataPKColumn.getViewConstant() != null) {
                    bitSet.set(i);
                    nDataPKColumns--;
                }
            }
        }
        this.indexTableName = indexTableName;
        this.indexedColumnTypes = Lists.<PDataType>newArrayListWithExpectedSize(nIndexPKColumns-nDataPKColumns);
        this.indexedExpressions = Lists.newArrayListWithExpectedSize(nIndexPKColumns-nDataPKColumns);
        this.coveredColumnsMap = Maps.newHashMapWithExpectedSize(nIndexColumns - nIndexPKColumns);
        this.nIndexSaltBuckets  = nIndexSaltBuckets == null ? 0 : nIndexSaltBuckets;
        this.dataEmptyKeyValueCF = SchemaUtil.getEmptyColumnFamily(dataTable);
        this.emptyKeyValueCFPtr = SchemaUtil.getEmptyColumnFamilyPtr(index);
        this.nDataCFs = dataTable.getColumnFamilies().size();
        this.indexWALDisabled = indexWALDisabled;
        // TODO: check whether index is immutable or not. Currently it's always false so checking
        // data table is with immutable rows or not.
        this.immutableRows = dataTable.isImmutableRows();
        int indexColByteSize = 0;
        ColumnResolver resolver = null;
        List<ParseNode> parseNodes = new ArrayList<ParseNode>(1);
        UDFParseNodeVisitor visitor = new UDFParseNodeVisitor();
        for (int i = indexPosOffset; i < index.getPKColumns().size(); i++) {
            PColumn indexColumn = index.getPKColumns().get(i);
            String expressionStr = IndexUtil.getIndexColumnExpressionStr(indexColumn);
            try {
                ParseNode parseNode  = SQLParser.parseCondition(expressionStr);
                parseNode.accept(visitor);
                parseNodes.add(parseNode);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            resolver = FromCompiler.getResolver(connection, new TableRef(dataTable), visitor.getUdfParseNodes());
        } catch (SQLException e) {
            throw new RuntimeException(e); // Impossible
        }
        StatementContext context = new StatementContext(new PhoenixStatement(connection), resolver);
        this.indexedColumnsInfo = Sets.newHashSetWithExpectedSize(nIndexColumns - nIndexPKColumns);
        
        IndexExpressionCompiler expressionIndexCompiler = new IndexExpressionCompiler(context);
        for (int i = indexPosOffset; i < index.getPKColumns().size(); i++) {
            PColumn indexColumn = index.getPKColumns().get(i);
            int indexPos = i - indexPosOffset;
            Expression expression = null;
            try {
                expressionIndexCompiler.reset();
                expression = parseNodes.get(indexPos).accept(expressionIndexCompiler);
            } catch (SQLException e) {
                throw new RuntimeException(e); // Impossible
            }
            if ( expressionIndexCompiler.getColumnRef()!=null ) {
            	// get the column of the data column that corresponds to this index column
	            PColumn column = IndexUtil.getDataColumn(dataTable, indexColumn.getName().getString());
	            boolean isPKColumn = SchemaUtil.isPKColumn(column);
	            if (isPKColumn) {
	                int dataPkPos = dataTable.getPKColumns().indexOf(column) - (dataTable.getBucketNum() == null ? 0 : 1) - (this.isMultiTenant ? 1 : 0);
	                this.rowKeyMetaData.setIndexPkPosition(dataPkPos, indexPos);
	                indexedColumnsInfo.add(new Pair<>((String)null, column.getName().getString()));
	            } else {
	                indexColByteSize += column.getDataType().isFixedWidth() ? SchemaUtil.getFixedByteSize(column) : ValueSchema.ESTIMATED_VARIABLE_LENGTH_SIZE;
	                try {
	                    // Surround constant with cast so that we can still know the original type. Otherwise, if we lose the type,
	                    // (for example when VARCHAR becomes CHAR), it can lead to problems in the type translation we do between data tables and indexes.
	                    if (column.isNullable() && ExpressionUtil.isConstant(expression)) {
	                        expression = CoerceExpression.create(expression, indexColumn.getDataType());
	                    }
                        this.indexedExpressions.add(expression);
                        indexedColumnsInfo.add(new Pair<>(column.getFamilyName().getString(), column.getName().getString()));
                    } catch (SQLException e) {
                        throw new RuntimeException(e); // Impossible
                    }
	            }
            }
            else {
            	indexColByteSize += expression.getDataType().isFixedWidth() ? SchemaUtil.getFixedByteSize(expression) : ValueSchema.ESTIMATED_VARIABLE_LENGTH_SIZE;
                this.indexedExpressions.add(expression);
                KeyValueExpressionVisitor kvVisitor = new KeyValueExpressionVisitor() {
                    @Override
                    public Void visit(KeyValueColumnExpression colExpression) {
                        return addDataColInfo(dataTable, colExpression);
                    }

                    @Override
                    public Void visit(SingleCellColumnExpression expression) {
                        return addDataColInfo(dataTable, expression);
                    }

                    private Void addDataColInfo(final PTable dataTable, Expression expression) {
                        Preconditions.checkArgument(expression instanceof SingleCellColumnExpression
                                || expression instanceof KeyValueColumnExpression);

                        KeyValueColumnExpression colExpression = null;
                        if (expression instanceof SingleCellColumnExpression) {
                            colExpression =
                                    ((SingleCellColumnExpression) expression).getKeyValueExpression();
                        } else {
                            colExpression = ((KeyValueColumnExpression) expression);
                        }
                        byte[] cf = colExpression.getColumnFamily();
                        byte[] cq = colExpression.getColumnQualifier();
                        try {
                            PColumn dataColumn =
                                    cf == null ? dataTable.getColumnForColumnQualifier(null, cq)
                                            : dataTable.getColumnFamily(cf)
                                                    .getPColumnForColumnQualifier(cq);
                            indexedColumnsInfo.add(new Pair<>(dataColumn.getFamilyName()
                                    .getString(), dataColumn.getName().getString()));
                        } catch (ColumnNotFoundException | ColumnFamilyNotFoundException
                                | AmbiguousColumnException e) {
                            throw new RuntimeException(e);
                        }
                        return null;
                    }
                };
                expression.accept(kvVisitor);
            }
            // set the sort order of the expression correctly
            if (indexColumn.getSortOrder() == SortOrder.DESC) {
                this.rowKeyMetaData.getDescIndexColumnBitSet().set(indexPos);
            }
        }
        this.estimatedExpressionSize = expressionIndexCompiler.getTotalNodeCount() * ESTIMATED_EXPRESSION_SIZE;
        for (int i = 0; i < index.getColumnFamilies().size(); i++) {
            PColumnFamily family = index.getColumnFamilies().get(i);
            for (PColumn indexColumn : family.getColumns()) {
                PColumn dataColumn = IndexUtil.getDataColumn(dataTable, indexColumn.getName().getString());
                byte[] dataColumnCq = dataColumn.getColumnQualifierBytes();
                byte[] indexColumnCq = indexColumn.getColumnQualifierBytes();
                this.coveredColumnsMap.put(new ColumnReference(dataColumn.getFamilyName().getBytes(), dataColumnCq), 
                        new ColumnReference(indexColumn.getFamilyName().getBytes(), indexColumnCq));
            }
        }
        this.estimatedIndexRowKeyBytes = estimateIndexRowKeyByteSize(indexColByteSize);
        initCachedState();
    }
    
    public byte[] buildRowKey(ValueGetter valueGetter, ImmutableBytesWritable rowKeyPtr, byte[] regionStartKey, byte[] regionEndKey)  {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        boolean prependRegionStartKey = isLocalIndex && regionStartKey != null;
        boolean isIndexSalted = !isLocalIndex && nIndexSaltBuckets > 0;
        int prefixKeyLength =
                prependRegionStartKey ? (regionStartKey.length != 0 ? regionStartKey.length
                        : regionEndKey.length) : 0; 
        TrustedByteArrayOutputStream stream = new TrustedByteArrayOutputStream(estimatedIndexRowKeyBytes + (prependRegionStartKey ? prefixKeyLength : 0));
        DataOutput output = new DataOutputStream(stream);
        try {
            // For local indexes, we must prepend the row key with the start region key
            if (prependRegionStartKey) {
                if (regionStartKey.length == 0) {
                    output.write(new byte[prefixKeyLength]);
                } else {
                    output.write(regionStartKey);
                }
            }
            if (isIndexSalted) {
                output.write(0); // will be set at end to index salt byte
            }
            // The dataRowKeySchema includes the salt byte field,
            // so we must adjust for that here.
            int dataPosOffset = isDataTableSalted ? 1 : 0 ;
            BitSet viewConstantColumnBitSet = this.rowKeyMetaData.getViewConstantColumnBitSet();
            int nIndexedColumns = getIndexPkColumnCount() - getNumViewConstants();
            int[][] dataRowKeyLocator = new int[2][nIndexedColumns];
            // Skip data table salt byte
            int maxRowKeyOffset = rowKeyPtr.getOffset() + rowKeyPtr.getLength();
            dataRowKeySchema.iterator(rowKeyPtr, ptr, dataPosOffset);
            
            if (viewIndexId != null) {
                output.write(viewIndexId);
            }
            
            if (isMultiTenant) {
                dataRowKeySchema.next(ptr, dataPosOffset, maxRowKeyOffset);
                output.write(ptr.get(), ptr.getOffset(), ptr.getLength());
                if (!dataRowKeySchema.getField(dataPosOffset).getDataType().isFixedWidth()) {
                    output.writeByte(SchemaUtil.getSeparatorByte(rowKeyOrderOptimizable, ptr.getLength()==0, dataRowKeySchema.getField(dataPosOffset)));
                }
                dataPosOffset++;
            }
            
            // Write index row key
            for (int i = dataPosOffset; i < dataRowKeySchema.getFieldCount(); i++) {
                Boolean hasValue=dataRowKeySchema.next(ptr, i, maxRowKeyOffset);
                // Ignore view constants from the data table, as these
                // don't need to appear in the index (as they're the
                // same for all rows in this index)
                if (!viewConstantColumnBitSet.get(i)) {
                    int pos = rowKeyMetaData.getIndexPkPosition(i-dataPosOffset);
                    if (Boolean.TRUE.equals(hasValue)) {
                        dataRowKeyLocator[0][pos] = ptr.getOffset();
                        dataRowKeyLocator[1][pos] = ptr.getLength();
                    } else {
                        dataRowKeyLocator[0][pos] = 0;
                        dataRowKeyLocator[1][pos] = 0;
                    }
                } 
            }
            BitSet descIndexColumnBitSet = rowKeyMetaData.getDescIndexColumnBitSet();
            Iterator<Expression> expressionIterator = indexedExpressions.iterator();
            for (int i = 0; i < nIndexedColumns; i++) {
                PDataType dataColumnType;
                boolean isNullable;
                SortOrder dataSortOrder;
                if (dataPkPosition[i] == EXPRESSION_NOT_PRESENT) {
                	Expression expression = expressionIterator.next();
                	dataColumnType = expression.getDataType();
                	dataSortOrder = expression.getSortOrder();
                    isNullable = expression.isNullable();
                	expression.evaluate(new ValueGetterTuple(valueGetter), ptr);
                }
                else {
                    Field field = dataRowKeySchema.getField(dataPkPosition[i]);
                    dataColumnType = field.getDataType();
                    ptr.set(rowKeyPtr.get(), dataRowKeyLocator[0][i], dataRowKeyLocator[1][i]);
                    dataSortOrder = field.getSortOrder();
                    isNullable = field.isNullable();
                }
                boolean isDataColumnInverted = dataSortOrder != SortOrder.ASC;
                PDataType indexColumnType = IndexUtil.getIndexColumnDataType(isNullable, dataColumnType);
                boolean isBytesComparable = dataColumnType.isBytesComparableWith(indexColumnType);
                boolean isIndexColumnDesc = descIndexColumnBitSet.get(i);
                if (isBytesComparable && isDataColumnInverted == isIndexColumnDesc) {
                    output.write(ptr.get(), ptr.getOffset(), ptr.getLength());
                } else {
                    if (!isBytesComparable)  {
                        indexColumnType.coerceBytes(ptr, dataColumnType, dataSortOrder, SortOrder.getDefault());
                    }
                    if (isDataColumnInverted != isIndexColumnDesc) {
                        writeInverted(ptr.get(), ptr.getOffset(), ptr.getLength(), output);
                    } else {
                        output.write(ptr.get(), ptr.getOffset(), ptr.getLength());
                    }
                }
                if (!indexColumnType.isFixedWidth()) {
                    output.writeByte(SchemaUtil.getSeparatorByte(rowKeyOrderOptimizable, ptr.getLength() == 0, isIndexColumnDesc ? SortOrder.DESC : SortOrder.ASC));
                }
            }
            int length = stream.size();
            int minLength = length - maxTrailingNulls;
            byte[] indexRowKey = stream.getBuffer();
            // Remove trailing nulls
            while (length > minLength && indexRowKey[length-1] == QueryConstants.SEPARATOR_BYTE) {
                length--;
            }
            if (isIndexSalted) {
                // Set salt byte
                byte saltByte = SaltingUtil.getSaltingByte(indexRowKey, SaltingUtil.NUM_SALTING_BYTES, length-SaltingUtil.NUM_SALTING_BYTES, nIndexSaltBuckets);
                indexRowKey[0] = saltByte;
            }
            return indexRowKey.length == length ? indexRowKey : Arrays.copyOf(indexRowKey, length);
        } catch (IOException e) {
            throw new RuntimeException(e); // Impossible
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e); // Impossible
            }
        }
    }

    /*
     * Build the data row key from the index row key
     */
    public byte[] buildDataRowKey(ImmutableBytesWritable indexRowKeyPtr, byte[][] viewConstants)  {
        RowKeySchema indexRowKeySchema = getIndexRowKeySchema();
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        TrustedByteArrayOutputStream stream = new TrustedByteArrayOutputStream(estimatedIndexRowKeyBytes);
        DataOutput output = new DataOutputStream(stream);
        // Increment dataPosOffset until all have been written
        int dataPosOffset = 0;
        int viewConstantsIndex = 0;
        try {
            int indexPosOffset = !isLocalIndex && nIndexSaltBuckets > 0 ? 1 : 0;
            int maxRowKeyOffset = indexRowKeyPtr.getOffset() + indexRowKeyPtr.getLength();
            indexRowKeySchema.iterator(indexRowKeyPtr, ptr, indexPosOffset);
            if (isDataTableSalted) {
                dataPosOffset++;
                output.write(0); // will be set at end to salt byte
            }
            if (isMultiTenant) {
                indexRowKeySchema.next(ptr, indexPosOffset, maxRowKeyOffset);
                output.write(ptr.get(), ptr.getOffset(), ptr.getLength());
                if (!dataRowKeySchema.getField(dataPosOffset).getDataType().isFixedWidth()) {
                    output.writeByte(SchemaUtil.getSeparatorByte(rowKeyOrderOptimizable, ptr.getLength() == 0, dataRowKeySchema.getField(dataPosOffset)));
                }
                indexPosOffset++;
                dataPosOffset++;
            }
            indexPosOffset = (!isLocalIndex && nIndexSaltBuckets > 0 ? 1 : 0) + (isMultiTenant ? 1 : 0) + (viewIndexId == null ? 0 : 1);
            BitSet viewConstantColumnBitSet = this.rowKeyMetaData.getViewConstantColumnBitSet();
            BitSet descIndexColumnBitSet = rowKeyMetaData.getDescIndexColumnBitSet();
            for (int i = dataPosOffset; i < dataRowKeySchema.getFieldCount(); i++) {
                // Write view constants from the data table, as these
                // won't appear in the index (as they're the
                // same for all rows in this index)
                if (viewConstantColumnBitSet.get(i)) {
                    output.write(viewConstants[viewConstantsIndex++]);
                } else {
                    int pos = rowKeyMetaData.getIndexPkPosition(i-dataPosOffset);
                    Boolean hasValue=indexRowKeySchema.iterator(indexRowKeyPtr, ptr, pos + indexPosOffset+1);
                    if (Boolean.TRUE.equals(hasValue)) {
                        // Write data row key value taking into account coercion and inversion
                        // if necessary
                        Field dataField = dataRowKeySchema.getField(i);
                        Field indexField = indexRowKeySchema.getField(pos + indexPosOffset);
                        PDataType indexColumnType = indexField.getDataType();
                        PDataType dataColumnType = dataField.getDataType();
                        SortOrder dataSortOrder = dataField.getSortOrder();
                        SortOrder indexSortOrder = indexField.getSortOrder();
                        boolean isDataColumnInverted = dataSortOrder != SortOrder.ASC;
                        boolean isBytesComparable = dataColumnType.isBytesComparableWith(indexColumnType) ;
                        if (isBytesComparable && isDataColumnInverted == descIndexColumnBitSet.get(pos)) {
                            output.write(ptr.get(), ptr.getOffset(), ptr.getLength());
                        } else {
                            if (!isBytesComparable)  {
                                dataColumnType.coerceBytes(ptr, indexColumnType, indexSortOrder, SortOrder.getDefault());
                            }
                            if (descIndexColumnBitSet.get(pos) != isDataColumnInverted) {
                                writeInverted(ptr.get(), ptr.getOffset(), ptr.getLength(), output);
                            } else {
                                output.write(ptr.get(), ptr.getOffset(), ptr.getLength());
                            }
                        }
                    }
                }
                // Write separator byte if variable length unless it's the last field in the schema
                // (but we still need to write it if it's DESC to ensure sort order is correct).
                byte sepByte = SchemaUtil.getSeparatorByte(rowKeyOrderOptimizable, ptr.getLength() == 0, dataRowKeySchema.getField(i));
                if (!dataRowKeySchema.getField(i).getDataType().isFixedWidth() && (((i+1) !=  dataRowKeySchema.getFieldCount()) || sepByte == QueryConstants.DESC_SEPARATOR_BYTE)) {
                    output.writeByte(sepByte);
                }
            }
            int length = stream.size();
            int minLength = length - maxTrailingNulls;
            byte[] dataRowKey = stream.getBuffer();
            // Remove trailing nulls
            while (length > minLength && dataRowKey[length-1] == QueryConstants.SEPARATOR_BYTE) {
                length--;
            }
            // TODO: need to capture nDataSaltBuckets instead of just a boolean. For now,
            // we store this in nIndexSaltBuckets, as we only use this function for local indexes
            // in which case nIndexSaltBuckets would never be used. Note that when we do add this
            // to be serialized, we have to add it at the end and allow for the value not being
            // there to maintain compatibility between an old client and a new server.
            if (isDataTableSalted) {
                // Set salt byte
                byte saltByte = SaltingUtil.getSaltingByte(dataRowKey, SaltingUtil.NUM_SALTING_BYTES, length-SaltingUtil.NUM_SALTING_BYTES, nIndexSaltBuckets);
                dataRowKey[0] = saltByte;
            }
            return dataRowKey.length == length ? dataRowKey : Arrays.copyOf(dataRowKey, length);
        } catch (IOException e) {
            throw new RuntimeException(e); // Impossible
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e); // Impossible
            }
        }
    }
    
    /*
     * return the view index id from the index row key
     */
    public byte[] getViewIndexIdFromIndexRowKey(ImmutableBytesWritable indexRowKeyPtr) {
        assert(isLocalIndex);
        RowKeySchema indexRowKeySchema = getIndexRowKeySchema();
        // TODO add logic to skip region start key as well because we cannot find the region startkey in indexhalfstorefilereader.
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        TrustedByteArrayOutputStream stream =
                new TrustedByteArrayOutputStream(estimatedIndexRowKeyBytes);
        DataOutput output = new DataOutputStream(stream);
        try {
            int indexPosOffset = (!isLocalIndex && nIndexSaltBuckets > 0 ? 1 : 0) + (isMultiTenant ? 1 : 0) + (viewIndexId == null ? 0 : 1);
            Boolean hasValue =
                    indexRowKeySchema.iterator(indexRowKeyPtr, ptr, indexPosOffset);
            if (Boolean.TRUE.equals(hasValue)) {
                    output.write(ptr.get(), ptr.getOffset(), ptr.getLength());
            }
            int length = stream.size();
            byte[] dataRowKey = stream.getBuffer();
            return dataRowKey.length == length ? dataRowKey : Arrays.copyOf(dataRowKey, length);
        } catch (IOException e) {
            throw new RuntimeException(e); // Impossible
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e); // Impossible
            }
        }
    }

    private volatile RowKeySchema indexRowKeySchema;
    
    // We have enough information to generate the index row key schema
    private RowKeySchema generateIndexRowKeySchema() {
        int nIndexedColumns = getIndexPkColumnCount() + (isMultiTenant ? 1 : 0) + (!isLocalIndex && nIndexSaltBuckets > 0 ? 1 : 0) + (viewIndexId != null ? 1 : 0) - getNumViewConstants();
        RowKeySchema.RowKeySchemaBuilder builder = new RowKeySchema.RowKeySchemaBuilder(nIndexedColumns);
        builder.rowKeyOrderOptimizable(rowKeyOrderOptimizable);
        if (!isLocalIndex && nIndexSaltBuckets > 0) {
            builder.addField(SaltingUtil.SALTING_COLUMN, false, SortOrder.ASC);
            nIndexedColumns--;
        }
        int dataPosOffset = isDataTableSalted ? 1 : 0 ;
        if (viewIndexId != null) {
            nIndexedColumns--;
            builder.addField(new PDatum() {

                @Override
                public boolean isNullable() {
                    return false;
                }

                @Override
                public PDataType getDataType() {
                    return MetaDataUtil.getViewIndexIdDataType();
                }

                @Override
                public Integer getMaxLength() {
                    return null;
                }

                @Override
                public Integer getScale() {
                    return null;
                }

                @Override
                public SortOrder getSortOrder() {
                    return SortOrder.getDefault();
                }
                
            }, false, SortOrder.getDefault());
        }
        if (isMultiTenant) {
            Field field = dataRowKeySchema.getField(dataPosOffset++);
            builder.addField(field, field.isNullable(), field.getSortOrder());
            nIndexedColumns--;
        }
        
        Field[] indexFields = new Field[nIndexedColumns];
        BitSet viewConstantColumnBitSet = this.rowKeyMetaData.getViewConstantColumnBitSet();
        // Add Field for all data row pk columns
        for (int i = dataPosOffset; i < dataRowKeySchema.getFieldCount(); i++) {
            // Ignore view constants from the data table, as these
            // don't need to appear in the index (as they're the
            // same for all rows in this index)
            if (!viewConstantColumnBitSet.get(i)) {
                int pos = rowKeyMetaData.getIndexPkPosition(i-dataPosOffset);
                indexFields[pos] = 
                        dataRowKeySchema.getField(i);
            } 
        }
        BitSet descIndexColumnBitSet = rowKeyMetaData.getDescIndexColumnBitSet();
        Iterator<Expression> expressionItr = indexedExpressions.iterator();
        for (int i = 0; i < indexFields.length; i++) {
            Field indexField = indexFields[i];
            PDataType dataTypeToBe;
            SortOrder sortOrderToBe;
            boolean isNullableToBe;
            Integer maxLengthToBe;
            Integer scaleToBe;
            if (indexField == null) {
                Expression e = expressionItr.next();
                isNullableToBe = e.isNullable();
                dataTypeToBe = IndexUtil.getIndexColumnDataType(isNullableToBe, e.getDataType());
                sortOrderToBe = descIndexColumnBitSet.get(i) ? SortOrder.DESC : SortOrder.ASC;
                maxLengthToBe = e.getMaxLength();
                scaleToBe = e.getScale();
            } else {
                isNullableToBe = indexField.isNullable();
                dataTypeToBe = IndexUtil.getIndexColumnDataType(isNullableToBe, indexField.getDataType());
                sortOrderToBe = descIndexColumnBitSet.get(i) ? SortOrder.DESC : SortOrder.ASC;
                maxLengthToBe = indexField.getMaxLength();
                scaleToBe = indexField.getScale();
            }
            final PDataType dataType = dataTypeToBe;
            final SortOrder sortOrder = sortOrderToBe;
            final boolean isNullable = isNullableToBe;
            final Integer maxLength = maxLengthToBe;
            final Integer scale = scaleToBe;
            builder.addField(new PDatum() {

                @Override
                public boolean isNullable() {
                    return isNullable;
                }

                @Override
                public PDataType getDataType() {
                    return dataType;
                }

                @Override
                public Integer getMaxLength() {
                    return maxLength;
                }

                @Override
                public Integer getScale() {
                    return scale;
                }

                @Override
                public SortOrder getSortOrder() {
                    return sortOrder;
                }
                
            }, true, sortOrder);
        }
        return builder.build();
    }
    
    private int getNumViewConstants() {
        BitSet bitSet = this.rowKeyMetaData.getViewConstantColumnBitSet();
        int num = 0;
        for(int i = 0; i<dataRowKeySchema.getFieldCount();i++){
            if(bitSet.get(i)) num++;
        }
        return num;
    }

    private RowKeySchema getIndexRowKeySchema() {
        if (indexRowKeySchema != null) {
            return indexRowKeySchema;
        }
        synchronized (this) {
            if (indexRowKeySchema == null) {
                indexRowKeySchema = generateIndexRowKeySchema();
            }
        }
        return indexRowKeySchema;
    }
    
    public Put buildUpdateMutation(KeyValueBuilder kvBuilder, ValueGetter valueGetter, ImmutableBytesWritable dataRowKeyPtr, long ts, byte[] regionStartKey, byte[] regionEndKey) throws IOException {
        byte[] indexRowKey = this.buildRowKey(valueGetter, dataRowKeyPtr, regionStartKey, regionEndKey);
        Put put = null;
        // New row being inserted: add the empty key value
        if (valueGetter==null || valueGetter.getLatestValue(dataEmptyKeyValueRef) == null) {
            put = new Put(indexRowKey);
            // add the keyvalue for the empty row
            put.add(kvBuilder.buildPut(new ImmutableBytesPtr(indexRowKey),
                this.getEmptyKeyValueFamily(), dataEmptyKeyValueRef.getQualifierWritable(), ts,
                // set the value to the empty column name
                dataEmptyKeyValueRef.getQualifierWritable()));
            put.setDurability(!indexWALDisabled ? Durability.USE_DEFAULT : Durability.SKIP_WAL);
        }
        ImmutableBytesPtr rowKey = new ImmutableBytesPtr(indexRowKey);
        if (immutableStorageScheme != ImmutableStorageScheme.ONE_CELL_PER_COLUMN) {
            // map from index column family to list of pair of index column and data column (for covered columns)
            Map<ImmutableBytesPtr, List<Pair<ColumnReference, ColumnReference>>> familyToColListMap = Maps.newHashMap();
            for (ColumnReference ref : this.getCoveredColumns()) {
                ColumnReference indexColRef = this.coveredColumnsMap.get(ref);
                ImmutableBytesPtr cf = new ImmutableBytesPtr(indexColRef.getFamily());
                if (!familyToColListMap.containsKey(cf)) {
                    familyToColListMap.put(cf, Lists.<Pair<ColumnReference, ColumnReference>>newArrayList());
                }
                familyToColListMap.get(cf).add(Pair.newPair(indexColRef, ref));
            }
            // iterate over each column family and create a byte[] containing all the columns 
            for (Entry<ImmutableBytesPtr, List<Pair<ColumnReference, ColumnReference>>> entry : familyToColListMap.entrySet()) {
                byte[] columnFamily = entry.getKey().copyBytesIfNecessary();
                List<Pair<ColumnReference, ColumnReference>> colRefPairs = entry.getValue();
                int maxEncodedColumnQualifier = Integer.MIN_VALUE;
                // find the max col qualifier
                for (Pair<ColumnReference, ColumnReference> colRefPair : colRefPairs) {
                    maxEncodedColumnQualifier = Math.max(maxEncodedColumnQualifier, encodingScheme.decode(colRefPair.getFirst().getQualifier()));
                }
                Expression[] colValues = EncodedColumnsUtil.createColumnExpressionArray(maxEncodedColumnQualifier);
                // set the values of the columns
                for (Pair<ColumnReference, ColumnReference> colRefPair : colRefPairs) {
                    ColumnReference indexColRef = colRefPair.getFirst();
                    ColumnReference dataColRef = colRefPair.getSecond();
                    Expression expression = new SingleCellColumnExpression(new PDatum() {
                        @Override
                        public boolean isNullable() {
                            return false;
                        }
                        
                        @Override
                        public SortOrder getSortOrder() {
                            return null;
                        }
                        
                        @Override
                        public Integer getScale() {
                            return null;
                        }
                        
                        @Override
                        public Integer getMaxLength() {
                            return null;
                        }
                        
                        @Override
                        public PDataType getDataType() {
                            return null;
                        }
                    }, dataColRef.getFamily(), dataColRef.getQualifier(), encodingScheme);
                    ImmutableBytesPtr ptr = new ImmutableBytesPtr();
                    expression.evaluate(new ValueGetterTuple(valueGetter), ptr);
                    byte[] value = ptr.copyBytesIfNecessary();
                    if (value != null) {
                        int indexArrayPos = encodingScheme.decode(indexColRef.getQualifier())-QueryConstants.ENCODED_CQ_COUNTER_INITIAL_VALUE+1;
                        colValues[indexArrayPos] = new LiteralExpression(value);
                    }
                }
                
                List<Expression> children = Arrays.asList(colValues);
                // we use SingleCellConstructorExpression to serialize multiple columns into a single byte[]
                SingleCellConstructorExpression singleCellConstructorExpression = new SingleCellConstructorExpression(immutableStorageScheme, children);
                ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                singleCellConstructorExpression.evaluate(new BaseTuple() {}, ptr);
                if (put == null) {
                    put = new Put(indexRowKey);
                    put.setDurability(!indexWALDisabled ? Durability.USE_DEFAULT : Durability.SKIP_WAL);
                }
                ImmutableBytesPtr colFamilyPtr = new ImmutableBytesPtr(columnFamily);
                //this is a little bit of extra work for installations that are running <0.94.14, but that should be rare and is a short-term set of wrappers - it shouldn't kill GC
                put.add(kvBuilder.buildPut(rowKey, colFamilyPtr, QueryConstants.SINGLE_KEYVALUE_COLUMN_QUALIFIER_BYTES_PTR, ts, ptr));
            }
        } else {
            for (ColumnReference ref : this.getCoveredColumns()) {
                ColumnReference indexColRef = this.coveredColumnsMap.get(ref);
                ImmutableBytesPtr cq = indexColRef.getQualifierWritable();
                ImmutableBytesPtr cf = indexColRef.getFamilyWritable();
                ImmutableBytesWritable value = valueGetter.getLatestValue(ref);
                if (value != null) {
                    if (put == null) {
                        put = new Put(indexRowKey);
                        put.setDurability(!indexWALDisabled ? Durability.USE_DEFAULT : Durability.SKIP_WAL);
                    }
                    put.add(kvBuilder.buildPut(rowKey, cf, cq, ts, value));
                }
            }
        }
        return put;
    }

    private enum DeleteType {SINGLE_VERSION, ALL_VERSIONS};
    private DeleteType getDeleteTypeOrNull(Collection<KeyValue> pendingUpdates) {
        int nDeleteCF = 0;
        int nDeleteVersionCF = 0;
        for (KeyValue kv : pendingUpdates) {
        	if (kv.getTypeByte() == KeyValue.Type.DeleteFamilyVersion.getCode()) {
                nDeleteVersionCF++;
            }
        	else if (kv.getTypeByte() == KeyValue.Type.DeleteFamily.getCode()
        			// Since we don't include the index rows in the change set for txn tables, we need to detect row deletes that have transformed by TransactionProcessor
        			|| (CellUtil.matchingQualifier(kv, TxConstants.FAMILY_DELETE_QUALIFIER) && CellUtil.matchingValue(kv, HConstants.EMPTY_BYTE_ARRAY))) {
        	    nDeleteCF++;
        	}
        }
        // This is what a delete looks like on the server side for mutable indexing...
        // Should all be one or the other for DeleteFamily versus DeleteFamilyVersion, but just in case not
        DeleteType deleteType = null; 
        if (nDeleteVersionCF > 0 && nDeleteVersionCF >= this.nDataCFs) {
        	deleteType = DeleteType.SINGLE_VERSION;
        } else {
			int nDelete = nDeleteCF + nDeleteVersionCF;
			if (nDelete>0 && nDelete >= this.nDataCFs) {
				deleteType = DeleteType.ALL_VERSIONS;
			}
		}
        return deleteType;
    }
    
    public boolean isRowDeleted(Collection<KeyValue> pendingUpdates) {
        return getDeleteTypeOrNull(pendingUpdates) != null;
    }
    
    private boolean hasIndexedColumnChanged(ValueGetter oldState, Collection<KeyValue> pendingUpdates) throws IOException {
        if (pendingUpdates.isEmpty()) {
            return false;
        }
        Map<ColumnReference,Cell> newState = Maps.newHashMapWithExpectedSize(pendingUpdates.size()); 
        for (Cell kv : pendingUpdates) {
            newState.put(new ColumnReference(CellUtil.cloneFamily(kv), CellUtil.cloneQualifier(kv)), kv);
        }
        for (ColumnReference ref : indexedColumns) {
        	Cell newValue = newState.get(ref);
        	if (newValue != null) { // Indexed column has potentially changed
        	    ImmutableBytesWritable oldValue = oldState.getLatestValue(ref);
        		boolean newValueSetAsNull = (newValue.getTypeByte() == Type.DeleteColumn.getCode() || newValue.getTypeByte() == Type.Delete.getCode() || CellUtil.matchingValue(newValue, HConstants.EMPTY_BYTE_ARRAY));
        		//If the new column value has to be set as null and the older value is null too,
        		//then just skip to the next indexed column.
        		if (newValueSetAsNull && oldValue == null) {
        			continue;
        		}
        		if ((oldValue == null && !newValueSetAsNull) || (oldValue != null && newValueSetAsNull)) {
        			return true;
        		}
        		// If the old value is different than the new value, the index row needs to be deleted
        		if (Bytes.compareTo(oldValue.get(), oldValue.getOffset(), oldValue.getLength(), 
        				newValue.getValueArray(), newValue.getValueOffset(), newValue.getValueLength()) != 0) {
        			return true;
        		}
        	}
        }
        return false;
    }
    
   /**
     * Used for immutable indexes that only index PK column values. In that case, we can handle a data row deletion,
     * since we can build the corresponding index row key.
     */
    public Delete buildDeleteMutation(KeyValueBuilder kvBuilder, ImmutableBytesWritable dataRowKeyPtr, long ts) throws IOException {
        return buildDeleteMutation(kvBuilder, null, dataRowKeyPtr, Collections.<KeyValue>emptyList(), ts, null, null);
    }
    
    public Delete buildDeleteMutation(KeyValueBuilder kvBuilder, ValueGetter oldState, ImmutableBytesWritable dataRowKeyPtr, Collection<KeyValue> pendingUpdates, long ts, byte[] regionStartKey, byte[] regionEndKey) throws IOException {
        byte[] indexRowKey = this.buildRowKey(oldState, dataRowKeyPtr, regionStartKey, regionEndKey);
        // Delete the entire row if any of the indexed columns changed
        DeleteType deleteType = null;
        if (oldState == null || (deleteType=getDeleteTypeOrNull(pendingUpdates)) != null || hasIndexedColumnChanged(oldState, pendingUpdates)) { // Deleting the entire row
            byte[] emptyCF = emptyKeyValueCFPtr.copyBytesIfNecessary();
            Delete delete = new Delete(indexRowKey);
            
            for (ColumnReference ref : getCoveredColumns()) {
                ColumnReference indexColumn = coveredColumnsMap.get(ref);
                // If table delete was single version, then index delete should be as well
                if (deleteType == DeleteType.SINGLE_VERSION) {
                    delete.deleteFamilyVersion(indexColumn.getFamily(), ts);
                } else {
                    delete.deleteFamily(indexColumn.getFamily(), ts);
                }
            }
            if (deleteType == DeleteType.SINGLE_VERSION) {
                delete.deleteFamilyVersion(emptyCF, ts);
            } else {
                delete.deleteFamily(emptyCF, ts);
            }
            delete.setDurability(!indexWALDisabled ? Durability.USE_DEFAULT : Durability.SKIP_WAL);
            return delete;
        }
        Delete delete = null;
        Set<ColumnReference> dataTableColRefs = coveredColumnsMap.keySet();
        // Delete columns for missing key values
        for (Cell kv : pendingUpdates) {
            if (kv.getTypeByte() != KeyValue.Type.Put.getCode()) {
                ColumnReference ref = new ColumnReference(kv.getFamily(), kv.getQualifier());
                if (dataTableColRefs.contains(ref)) {
                    if (delete == null) {
                        delete = new Delete(indexRowKey);                    
                        delete.setDurability(!indexWALDisabled ? Durability.USE_DEFAULT : Durability.SKIP_WAL);
                    }
                    ColumnReference indexColumn = coveredColumnsMap.get(ref);
                    // If point delete for data table, then use point delete for index as well
                    if (kv.getTypeByte() == KeyValue.Type.Delete.getCode()) { 
                        delete.deleteColumn(indexColumn.getFamily(), indexColumn.getQualifier(), ts);
                    } else {
                        delete.deleteColumns(indexColumn.getFamily(), indexColumn.getQualifier(), ts);
                    }
                }
            }
        }
        return delete;
    }
    
    public byte[] getIndexTableName() {
        return indexTableName;
    }
    
    public Set<ColumnReference> getCoveredColumns() {
        return coveredColumnsMap.keySet();
    }

    public Set<ColumnReference> getAllColumns() {
        return allColumns;
    }
    
    public ImmutableBytesPtr getEmptyKeyValueFamily() {
        // Since the metadata of an index table will never change,
        // we can infer this based on the family of the first covered column
        // If if there are no covered columns, we know it's our default name
        return emptyKeyValueCFPtr;
    }
    
    @Deprecated // Only called by code older than our 4.10 release
    @Override
    public void readFields(DataInput input) throws IOException {
        int encodedIndexSaltBucketsAndMultiTenant = WritableUtils.readVInt(input);
        isMultiTenant = encodedIndexSaltBucketsAndMultiTenant < 0;
        nIndexSaltBuckets = Math.abs(encodedIndexSaltBucketsAndMultiTenant) - 1;
        int encodedIndexedColumnsAndViewId = WritableUtils.readVInt(input);
        boolean hasViewIndexId = encodedIndexedColumnsAndViewId < 0;
        if (hasViewIndexId) {
            // Fixed length
            viewIndexId = new byte[MetaDataUtil.getViewIndexIdDataType().getByteSize()];
            input.readFully(viewIndexId);
        }
        int nIndexedColumns = Math.abs(encodedIndexedColumnsAndViewId) - 1;
        indexedColumns = Sets.newLinkedHashSetWithExpectedSize(nIndexedColumns);
        for (int i = 0; i < nIndexedColumns; i++) {
            byte[] cf = Bytes.readByteArray(input);
            byte[] cq = Bytes.readByteArray(input);
            indexedColumns.add(new ColumnReference(cf,cq));
        }
        indexedColumnTypes = Lists.newArrayListWithExpectedSize(nIndexedColumns);
        for (int i = 0; i < nIndexedColumns; i++) {
            PDataType type = PDataType.values()[WritableUtils.readVInt(input)];
            indexedColumnTypes.add(type);
        }
        int encodedCoveredolumnsAndLocalIndex = WritableUtils.readVInt(input);
        isLocalIndex = encodedCoveredolumnsAndLocalIndex < 0;
        int nCoveredColumns = Math.abs(encodedCoveredolumnsAndLocalIndex) - 1;
        coveredColumnsMap = Maps.newHashMapWithExpectedSize(nCoveredColumns);
        for (int i = 0; i < nCoveredColumns; i++) {
            byte[] dataTableCf = Bytes.readByteArray(input);
            byte[] dataTableCq = Bytes.readByteArray(input);
            ColumnReference dataTableRef = new ColumnReference(dataTableCf, dataTableCq);
            byte[] indexTableCf = isLocalIndex ? IndexUtil.getLocalIndexColumnFamily(dataTableCf) : dataTableCf;
            byte[] indexTableCq  = IndexUtil.getIndexColumnName(dataTableCf, dataTableCq);
            ColumnReference indexTableRef = new ColumnReference(indexTableCf, indexTableCq);
            coveredColumnsMap.put(dataTableRef, indexTableRef);
        }
        // Hack to serialize whether the index row key is optimizable
        int len = WritableUtils.readVInt(input);
        if (len < 0) {
            rowKeyOrderOptimizable = false;
            len *= -1;
        } else {
            rowKeyOrderOptimizable = true;
        }
        indexTableName = new byte[len];
        input.readFully(indexTableName, 0, len);
        dataEmptyKeyValueCF = Bytes.readByteArray(input);
        len = WritableUtils.readVInt(input);
        //TODO remove this in the next major release
        boolean isNewClient = false;
        if (len < 0) {
          isNewClient = true;
          len=Math.abs(len);
        }
        byte [] emptyKeyValueCF = new byte[len];
        input.readFully(emptyKeyValueCF, 0, len);
        emptyKeyValueCFPtr = new ImmutableBytesPtr(emptyKeyValueCF);
        
        if (isNewClient) {
            int numIndexedExpressions = WritableUtils.readVInt(input);
            indexedExpressions = Lists.newArrayListWithExpectedSize(numIndexedExpressions);        
            for (int i = 0; i < numIndexedExpressions; i++) {
                Expression expression = ExpressionType.values()[WritableUtils.readVInt(input)].newInstance();
                expression.readFields(input);
                indexedExpressions.add(expression);
            }
        }
        else {
            indexedExpressions = Lists.newArrayListWithExpectedSize(indexedColumns.size());
            Iterator<ColumnReference> colReferenceIter = indexedColumns.iterator();
            Iterator<PDataType> dataTypeIter = indexedColumnTypes.iterator();
            while (colReferenceIter.hasNext()) {
                ColumnReference colRef = colReferenceIter.next();
                final PDataType dataType = dataTypeIter.next();
                indexedExpressions.add(new KeyValueColumnExpression(new PDatum() {
                    
                    @Override
                    public boolean isNullable() {
                        return true;
                    }
                    
                    @Override
                    public SortOrder getSortOrder() {
                        return SortOrder.getDefault();
                    }
                    
                    @Override
                    public Integer getScale() {
                        return null;
                    }
                    
                    @Override
                    public Integer getMaxLength() {
                        return null;
                    }
                    
                    @Override
                    public PDataType getDataType() {
                        return dataType;
                    }
                }, colRef.getFamily(), colRef.getQualifier()));
            }
        }
        
        rowKeyMetaData = newRowKeyMetaData();
        rowKeyMetaData.readFields(input);
        int nDataCFs = WritableUtils.readVInt(input);
        // Encode indexWALDisabled in nDataCFs
        indexWALDisabled = nDataCFs < 0;
        this.nDataCFs = Math.abs(nDataCFs) - 1;
        int encodedEstimatedIndexRowKeyBytesAndImmutableRows = WritableUtils.readVInt(input);
        this.immutableRows = encodedEstimatedIndexRowKeyBytesAndImmutableRows < 0;
        this.estimatedIndexRowKeyBytes = Math.abs(encodedEstimatedIndexRowKeyBytesAndImmutableRows);
        initCachedState();
    }
    
        
    public static IndexMaintainer fromProto(ServerCachingProtos.IndexMaintainer proto, RowKeySchema dataTableRowKeySchema, boolean isDataTableSalted) throws IOException {
        IndexMaintainer maintainer = new IndexMaintainer(dataTableRowKeySchema, isDataTableSalted);
        maintainer.nIndexSaltBuckets = proto.getSaltBuckets();
        maintainer.isMultiTenant = proto.getIsMultiTenant();
        maintainer.viewIndexId = proto.hasViewIndexId() ? proto.getViewIndexId().toByteArray() : null;
        List<ServerCachingProtos.ColumnReference> indexedColumnsList = proto.getIndexedColumnsList();
        maintainer.indexedColumns = new HashSet<ColumnReference>(indexedColumnsList.size());
        for (ServerCachingProtos.ColumnReference colRefFromProto : indexedColumnsList) {
            maintainer.indexedColumns.add(new ColumnReference(colRefFromProto.getFamily().toByteArray(), colRefFromProto.getQualifier().toByteArray()));
        }
        List<Integer> indexedColumnTypes = proto.getIndexedColumnTypeOrdinalList();
        maintainer.indexedColumnTypes = new ArrayList<PDataType>(indexedColumnTypes.size());
        for (Integer typeOrdinal : indexedColumnTypes) {
            maintainer.indexedColumnTypes.add(PDataType.values()[typeOrdinal]);
        }
        maintainer.indexTableName = proto.getIndexTableName().toByteArray();
        maintainer.rowKeyOrderOptimizable = proto.getRowKeyOrderOptimizable();
        maintainer.dataEmptyKeyValueCF = proto.getDataTableEmptyKeyValueColFamily().toByteArray();
        ServerCachingProtos.ImmutableBytesWritable emptyKeyValueColFamily = proto.getEmptyKeyValueColFamily();
        maintainer.emptyKeyValueCFPtr = new ImmutableBytesPtr(emptyKeyValueColFamily.getByteArray().toByteArray(), emptyKeyValueColFamily.getOffset(), emptyKeyValueColFamily.getLength());
        maintainer.indexedExpressions = new ArrayList<>();
        try (ByteArrayInputStream stream = new ByteArrayInputStream(proto.getIndexedExpressions().toByteArray())) {
            DataInput input = new DataInputStream(stream);
            while (stream.available() > 0) {
                int expressionOrdinal = WritableUtils.readVInt(input);
                Expression expression = ExpressionType.values()[expressionOrdinal].newInstance();
                expression.readFields(input);
                maintainer.indexedExpressions.add(expression);
            }
        }
        maintainer.rowKeyMetaData = newRowKeyMetaData(maintainer, dataTableRowKeySchema, maintainer.indexedExpressions.size(), isDataTableSalted, maintainer.isMultiTenant);
        try (ByteArrayInputStream stream = new ByteArrayInputStream(proto.getRowKeyMetadata().toByteArray())) {
            DataInput input = new DataInputStream(stream);
            maintainer.rowKeyMetaData.readFields(input);   
        }
        maintainer.nDataCFs = proto.getNumDataTableColFamilies();
        maintainer.indexWALDisabled = proto.getIndexWalDisabled();
        maintainer.estimatedIndexRowKeyBytes = proto.getIndexRowKeyByteSize();
        maintainer.immutableRows = proto.getImmutable();
        List<ColumnInfo> indexedColumnInfoList = proto.getIndexedColumnInfoList();
        maintainer.indexedColumnsInfo = Sets.newHashSet();
        for (ColumnInfo info : indexedColumnInfoList) {
            maintainer.indexedColumnsInfo.add(new Pair<>(info.getFamilyName(), info.getColumnName()));
        }
        // proto doesn't support single byte so need an explicit cast here
        maintainer.encodingScheme = PTable.QualifierEncodingScheme.fromSerializedValue((byte)proto.getEncodingScheme());
        maintainer.immutableStorageScheme = PTable.ImmutableStorageScheme.fromSerializedValue((byte)proto.getImmutableStorageScheme());
        maintainer.isLocalIndex = proto.getIsLocalIndex();
        
        List<ServerCachingProtos.ColumnReference> dataTableColRefsForCoveredColumnsList = proto.getDataTableColRefForCoveredColumnsList();
        List<ServerCachingProtos.ColumnReference> indexTableColRefsForCoveredColumnsList = proto.getIndexTableColRefForCoveredColumnsList();
        maintainer.coveredColumnsMap = Maps.newHashMapWithExpectedSize(dataTableColRefsForCoveredColumnsList.size());
        boolean encodedColumnNames = maintainer.encodingScheme != NON_ENCODED_QUALIFIERS;
        Iterator<ServerCachingProtos.ColumnReference> indexTableColRefItr = indexTableColRefsForCoveredColumnsList.iterator();
        for (ServerCachingProtos.ColumnReference colRefFromProto : dataTableColRefsForCoveredColumnsList) {
            ColumnReference dataTableColRef = new ColumnReference(colRefFromProto.getFamily().toByteArray(), colRefFromProto.getQualifier( ).toByteArray());
            ColumnReference indexTableColRef;
            if (encodedColumnNames) {
                ServerCachingProtos.ColumnReference fromProto = indexTableColRefItr.next(); 
                indexTableColRef = new ColumnReference(fromProto.getFamily().toByteArray(), fromProto.getQualifier( ).toByteArray());
            } else {
                byte[] cq = IndexUtil.getIndexColumnName(dataTableColRef.getFamily(), dataTableColRef.getQualifier());
                byte[] cf = maintainer.isLocalIndex ? IndexUtil.getLocalIndexColumnFamily(dataTableColRef.getFamily()) : dataTableColRef.getFamily();
                indexTableColRef = new ColumnReference(cf, cq);
            }
            maintainer.coveredColumnsMap.put(dataTableColRef, indexTableColRef);
        }
        maintainer.initCachedState();
        return maintainer;
    }
    
    @Deprecated // Only called by code older than our 4.10 release
    @Override
    public void write(DataOutput output) throws IOException {
        // Encode nIndexSaltBuckets and isMultiTenant together
        WritableUtils.writeVInt(output, (nIndexSaltBuckets + 1) * (isMultiTenant ? -1 : 1));
        // Encode indexedColumns.size() and whether or not there's a viewIndexId
        WritableUtils.writeVInt(output, (indexedColumns.size() + 1) * (viewIndexId != null ? -1 : 1));
        if (viewIndexId != null) {
            output.write(viewIndexId);
        }
        for (ColumnReference ref : indexedColumns) {
            Bytes.writeByteArray(output, ref.getFamily());
            Bytes.writeByteArray(output, ref.getQualifier());
        }
        //TODO remove indexedColumnTypes in the next major release
        for (int i = 0; i < indexedColumnTypes.size(); i++) {
            PDataType type = indexedColumnTypes.get(i);
            WritableUtils.writeVInt(output, type.ordinal());
        }
        // Encode coveredColumns.size() and whether or not this is a local index
        WritableUtils.writeVInt(output, (coveredColumnsMap.size() + 1) * (isLocalIndex ? -1 : 1));
        for (ColumnReference ref : coveredColumnsMap.keySet()) {
            Bytes.writeByteArray(output, ref.getFamily());
            Bytes.writeByteArray(output, ref.getQualifier());
        }
        // TODO: remove when rowKeyOrderOptimizable hack no longer needed
        WritableUtils.writeVInt(output,indexTableName.length * (rowKeyOrderOptimizable ? 1 : -1));
        output.write(indexTableName, 0, indexTableName.length);
        Bytes.writeByteArray(output, dataEmptyKeyValueCF);
        // TODO in order to maintain b/w compatibility encode emptyKeyValueCFPtr.getLength() as a negative value (so we can distinguish between new and old clients)
        // when indexedColumnTypes is removed, remove this 
        WritableUtils.writeVInt(output,-emptyKeyValueCFPtr.getLength());
        output.write(emptyKeyValueCFPtr.get(),emptyKeyValueCFPtr.getOffset(), emptyKeyValueCFPtr.getLength());
        
        WritableUtils.writeVInt(output, indexedExpressions.size());
        for (Expression expression : indexedExpressions) {
            WritableUtils.writeVInt(output, ExpressionType.valueOf(expression).ordinal());
            expression.write(output);
        }
        
        rowKeyMetaData.write(output);
        // Encode indexWALDisabled in nDataCFs
        WritableUtils.writeVInt(output, (nDataCFs + 1) * (indexWALDisabled ? -1 : 1));
        // Encode estimatedIndexRowKeyBytes and immutableRows together.
        WritableUtils.writeVInt(output, estimatedIndexRowKeyBytes * (immutableRows ? -1 : 1));
    }
    
    public static ServerCachingProtos.IndexMaintainer toProto(IndexMaintainer maintainer) throws IOException {
        ServerCachingProtos.IndexMaintainer.Builder builder = ServerCachingProtos.IndexMaintainer.newBuilder();
        builder.setSaltBuckets(maintainer.nIndexSaltBuckets);
        builder.setIsMultiTenant(maintainer.isMultiTenant);
        if (maintainer.viewIndexId != null) {
            builder.setViewIndexId(ByteStringer.wrap(maintainer.viewIndexId));
        }
        for (ColumnReference colRef : maintainer.indexedColumns) {
            ServerCachingProtos.ColumnReference.Builder cRefBuilder =  ServerCachingProtos.ColumnReference.newBuilder();
            cRefBuilder.setFamily(ByteStringer.wrap(colRef.getFamily()));
            cRefBuilder.setQualifier(ByteStringer.wrap(colRef.getQualifier()));
            builder.addIndexedColumns(cRefBuilder.build());
        }
        for (PDataType dataType : maintainer.indexedColumnTypes) {
            builder.addIndexedColumnTypeOrdinal(dataType.ordinal());
        }
        for (Entry<ColumnReference, ColumnReference> e : maintainer.coveredColumnsMap.entrySet()) {
            ServerCachingProtos.ColumnReference.Builder cRefBuilder =  ServerCachingProtos.ColumnReference.newBuilder();
            ColumnReference dataTableColRef = e.getKey();
            cRefBuilder.setFamily(ByteStringer.wrap(dataTableColRef.getFamily()));
            cRefBuilder.setQualifier(ByteStringer.wrap(dataTableColRef.getQualifier()));
            builder.addDataTableColRefForCoveredColumns(cRefBuilder.build());
            if (maintainer.encodingScheme != NON_ENCODED_QUALIFIERS) {
                // We need to serialize the colRefs of index tables only in case of encoded column names.
                ColumnReference indexTableColRef = e.getValue();
                cRefBuilder =  ServerCachingProtos.ColumnReference.newBuilder();
                cRefBuilder.setFamily(ByteStringer.wrap(indexTableColRef.getFamily()));
                cRefBuilder.setQualifier(ByteStringer.wrap(indexTableColRef.getQualifier()));
                builder.addIndexTableColRefForCoveredColumns(cRefBuilder.build());
            }
        }
        builder.setIsLocalIndex(maintainer.isLocalIndex);
        builder.setIndexTableName(ByteStringer.wrap(maintainer.indexTableName));
        builder.setRowKeyOrderOptimizable(maintainer.rowKeyOrderOptimizable);
        builder.setDataTableEmptyKeyValueColFamily(ByteStringer.wrap(maintainer.dataEmptyKeyValueCF));
        ServerCachingProtos.ImmutableBytesWritable.Builder ibwBuilder = ServerCachingProtos.ImmutableBytesWritable.newBuilder();
        ibwBuilder.setByteArray(ByteStringer.wrap(maintainer.emptyKeyValueCFPtr.get()));
        ibwBuilder.setLength(maintainer.emptyKeyValueCFPtr.getLength());
        ibwBuilder.setOffset(maintainer.emptyKeyValueCFPtr.getOffset());
        builder.setEmptyKeyValueColFamily(ibwBuilder.build());
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            DataOutput output = new DataOutputStream(stream);
            for (Expression expression : maintainer.indexedExpressions) {
                WritableUtils.writeVInt(output, ExpressionType.valueOf(expression).ordinal());
                expression.write(output);
            }
            builder.setIndexedExpressions(ByteStringer.wrap(stream.toByteArray()));
        }
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            DataOutput output = new DataOutputStream(stream);
            maintainer.rowKeyMetaData.write(output);
            builder.setRowKeyMetadata(ByteStringer.wrap(stream.toByteArray()));
        }
        builder.setNumDataTableColFamilies(maintainer.nDataCFs);
        builder.setIndexWalDisabled(maintainer.indexWALDisabled);
        builder.setIndexRowKeyByteSize(maintainer.estimatedIndexRowKeyBytes);
        builder.setImmutable(maintainer.immutableRows);
        for (Pair<String, String> p : maintainer.indexedColumnsInfo) {
            ServerCachingProtos.ColumnInfo.Builder ciBuilder = ServerCachingProtos.ColumnInfo.newBuilder();
            if (p.getFirst() != null) {
                ciBuilder.setFamilyName(p.getFirst());
            }
            ciBuilder.setColumnName(p.getSecond());
            builder.addIndexedColumnInfo(ciBuilder.build());
        }
        builder.setEncodingScheme(maintainer.encodingScheme.getSerializedMetadataValue());
        builder.setImmutableStorageScheme(maintainer.immutableStorageScheme.getSerializedMetadataValue());
        return builder.build();
    }

    public int getEstimatedByteSize() {
        int size = WritableUtils.getVIntSize(nIndexSaltBuckets);
        size += WritableUtils.getVIntSize(estimatedIndexRowKeyBytes);
        size += WritableUtils.getVIntSize(indexedColumns.size());
        size += viewIndexId == null ? 0 : viewIndexId.length;
        for (ColumnReference ref : indexedColumns) {
            size += WritableUtils.getVIntSize(ref.getFamily().length);
            size += ref.getFamily().length;
            size += WritableUtils.getVIntSize(ref.getQualifier().length);
            size += ref.getQualifier().length;
        }
        for (int i = 0; i < indexedColumnTypes.size(); i++) {
            PDataType type = indexedColumnTypes.get(i);
            size += WritableUtils.getVIntSize(type.ordinal());
        }
        Set<ColumnReference> dataTableColRefs = coveredColumnsMap.keySet();
        size += WritableUtils.getVIntSize(dataTableColRefs.size());
        for (ColumnReference ref : dataTableColRefs) {
            size += WritableUtils.getVIntSize(ref.getFamilyWritable().getSize());
            size += ref.getFamily().length;
            size += WritableUtils.getVIntSize(ref.getQualifierWritable().getSize());
            size += ref.getQualifier().length;
        }
        size += indexTableName.length + WritableUtils.getVIntSize(indexTableName.length);
        size += rowKeyMetaData.getByteSize();
        size += dataEmptyKeyValueCF.length + WritableUtils.getVIntSize(dataEmptyKeyValueCF.length);
        size += emptyKeyValueCFPtr.getLength() + WritableUtils.getVIntSize(emptyKeyValueCFPtr.getLength());
        size += WritableUtils.getVIntSize(nDataCFs+1);
        size += WritableUtils.getVIntSize(indexedExpressions.size());
        for (Expression expression : indexedExpressions) {
            size += WritableUtils.getVIntSize(ExpressionType.valueOf(expression).ordinal());
        }
        size += estimatedExpressionSize;
        return size;
    }
    
    private int estimateIndexRowKeyByteSize(int indexColByteSize) {
        int estimatedIndexRowKeyBytes = indexColByteSize + dataRowKeySchema.getEstimatedValueLength() + (nIndexSaltBuckets == 0 || isLocalIndex || this.isDataTableSalted ? 0 : SaltingUtil.NUM_SALTING_BYTES);
        return estimatedIndexRowKeyBytes;
    }
    
    /**
     * Init calculated state reading/creating
     */
    private void initCachedState() {
        byte[] emptyKvQualifier = EncodedColumnsUtil.getEmptyKeyValueInfo(encodingScheme).getFirst();
        dataEmptyKeyValueRef = new ColumnReference(emptyKeyValueCFPtr.copyBytesIfNecessary(), emptyKvQualifier);
        this.allColumns = Sets.newLinkedHashSetWithExpectedSize(indexedExpressions.size() + coveredColumnsMap.size());
        // columns that are required to evaluate all expressions in indexedExpressions (not including columns in data row key)
        this.indexedColumns = Sets.newLinkedHashSetWithExpectedSize(indexedExpressions.size());
        for (Expression expression : indexedExpressions) {
        	KeyValueExpressionVisitor visitor = new KeyValueExpressionVisitor() {
                @Override
                public Void visit(KeyValueColumnExpression expression) {
                	if (indexedColumns.add(new ColumnReference(expression.getColumnFamily(), expression.getColumnQualifier()))) {
                		indexedColumnTypes.add(expression.getDataType());
                	}
                    return null;
                }
            };
            expression.accept(visitor);
        }
        allColumns.addAll(indexedColumns);
        for (ColumnReference colRef : coveredColumnsMap.keySet()) {
            if (immutableStorageScheme==ImmutableStorageScheme.ONE_CELL_PER_COLUMN) {
                allColumns.add(colRef);
            } else {
                allColumns.add(new ColumnReference(colRef.getFamily(), QueryConstants.SINGLE_KEYVALUE_COLUMN_QUALIFIER_BYTES));
            }
        }
        
        int dataPkOffset = (isDataTableSalted ? 1 : 0) + (isMultiTenant ? 1 : 0);
        int nIndexPkColumns = getIndexPkColumnCount();
        dataPkPosition = new int[nIndexPkColumns];
        Arrays.fill(dataPkPosition, EXPRESSION_NOT_PRESENT);
        int numViewConstantColumns = 0;
        BitSet viewConstantColumnBitSet = rowKeyMetaData.getViewConstantColumnBitSet();
        for (int i = dataPkOffset; i < dataRowKeySchema.getFieldCount(); i++) {
            if (!viewConstantColumnBitSet.get(i)) {
                int indexPkPosition = rowKeyMetaData.getIndexPkPosition(i-dataPkOffset);
                this.dataPkPosition[indexPkPosition] = i;
            } else {
                numViewConstantColumns++;
            }
        }
        
        // Calculate the max number of trailing nulls that we should get rid of after building the index row key.
        // We only get rid of nulls for variable length types, so we have to be careful to consider the type of the
        // index table, not the data type of the data table
        int expressionsPos = indexedExpressions.size();
        int indexPkPos = nIndexPkColumns - numViewConstantColumns - 1;
        while (indexPkPos >= 0) {
            int dataPkPos = dataPkPosition[indexPkPos];
            boolean isDataNullable;
            PDataType dataType;
            if (dataPkPos == EXPRESSION_NOT_PRESENT) {
                isDataNullable = true;
                dataType = indexedExpressions.get(--expressionsPos).getDataType();
            } else {
                Field dataField = dataRowKeySchema.getField(dataPkPos);
                dataType = dataField.getDataType();
                isDataNullable = dataField.isNullable();
            }
            PDataType indexDataType = IndexUtil.getIndexColumnDataType(isDataNullable, dataType);
            if (indexDataType.isFixedWidth()) {
                break;
            }
            indexPkPos--;
        }
        maxTrailingNulls = nIndexPkColumns-indexPkPos-1;
    }

    private int getIndexPkColumnCount() {
        return getIndexPkColumnCount(dataRowKeySchema, indexedExpressions.size(), isDataTableSalted, isMultiTenant);
    }
    
    private static int getIndexPkColumnCount(RowKeySchema rowKeySchema, int numIndexExpressions, boolean isDataTableSalted, boolean isMultiTenant) {
        return rowKeySchema.getFieldCount() + numIndexExpressions - (isDataTableSalted ? 1 : 0) - (isMultiTenant ? 1 : 0);
    }
    
    private RowKeyMetaData newRowKeyMetaData() {
        return getIndexPkColumnCount() < 0xFF ? new ByteSizeRowKeyMetaData() : new IntSizedRowKeyMetaData();
    }
    
    private static RowKeyMetaData newRowKeyMetaData(IndexMaintainer i, RowKeySchema rowKeySchema, int numIndexExpressions, boolean isDataTableSalted, boolean isMultiTenant) {
        int indexPkColumnCount = getIndexPkColumnCount(rowKeySchema, numIndexExpressions, isDataTableSalted, isMultiTenant);
        return indexPkColumnCount < 0xFF ? i.new ByteSizeRowKeyMetaData() : i.new IntSizedRowKeyMetaData();
    }

    private RowKeyMetaData newRowKeyMetaData(int capacity) {
        return capacity < 0xFF ? new ByteSizeRowKeyMetaData(capacity) : new IntSizedRowKeyMetaData(capacity);
    }

    private static void writeInverted(byte[] buf, int offset, int length, DataOutput output) throws IOException {
        for (int i = offset; i < offset + length; i++) {
            byte b = SortOrder.invert(buf[i]);
            output.write(b);
        }
    }
    
    private abstract class RowKeyMetaData implements Writable {
        private BitSet descIndexColumnBitSet;
        private BitSet viewConstantColumnBitSet;
        
        private RowKeyMetaData() {
        }
        
        private RowKeyMetaData(int nIndexedColumns) {
            descIndexColumnBitSet = BitSet.withCapacity(nIndexedColumns);
            viewConstantColumnBitSet = BitSet.withCapacity(dataRowKeySchema.getMaxFields()); // Size based on number of data PK columns
      }
        
        protected int getByteSize() {
            return BitSet.getByteSize(getIndexPkColumnCount()) * 3 + BitSet.getByteSize(dataRowKeySchema.getMaxFields());
        }
        
        protected abstract int getIndexPkPosition(int dataPkPosition);
        protected abstract int setIndexPkPosition(int dataPkPosition, int indexPkPosition);
        
        @Override
        public void readFields(DataInput input) throws IOException {
            int length = getIndexPkColumnCount();
            descIndexColumnBitSet = BitSet.read(input, length);
            int vclength = dataRowKeySchema.getMaxFields();
            viewConstantColumnBitSet = BitSet.read(input, vclength);
        }
        
        @Override
        public void write(DataOutput output) throws IOException {
            int length = getIndexPkColumnCount();
            BitSet.write(output, descIndexColumnBitSet, length);
            int vclength = dataRowKeySchema.getMaxFields();
            BitSet.write(output, viewConstantColumnBitSet, vclength);
        }

        private BitSet getDescIndexColumnBitSet() {
            return descIndexColumnBitSet;
        }

        private BitSet getViewConstantColumnBitSet() {
            return viewConstantColumnBitSet;
        }
    }
    
    private static int BYTE_OFFSET = 127;
    
    private class ByteSizeRowKeyMetaData extends RowKeyMetaData {
        private byte[] indexPkPosition;
        
        private ByteSizeRowKeyMetaData() {
        }

        private ByteSizeRowKeyMetaData(int nIndexedColumns) {
            super(nIndexedColumns);
            this.indexPkPosition = new byte[nIndexedColumns];
        }
        
        @Override
        protected int getIndexPkPosition(int dataPkPosition) {
            // Use offset for byte so that we can get full range of 0 - 255
            // We use -128 as marker for a non row key index column,
            // that's why our offset if 127 instead of 128
            return this.indexPkPosition[dataPkPosition] + BYTE_OFFSET;
        }

        @Override
        protected int setIndexPkPosition(int dataPkPosition, int indexPkPosition) {
            return this.indexPkPosition[dataPkPosition] = (byte)(indexPkPosition - BYTE_OFFSET);
        }

        @Override
        public void write(DataOutput output) throws IOException {
            super.write(output);
            output.write(indexPkPosition);
        }

        @Override
        protected int getByteSize() {
            return super.getByteSize() + indexPkPosition.length;
        }

        @Override
        public void readFields(DataInput input) throws IOException {
            super.readFields(input);
            this.indexPkPosition = new byte[getIndexPkColumnCount()];
            input.readFully(indexPkPosition);
        }
    }
    
    private class IntSizedRowKeyMetaData extends RowKeyMetaData {
        private int[] indexPkPosition;
        
        private IntSizedRowKeyMetaData() {
        }

        private IntSizedRowKeyMetaData(int nIndexedColumns) {
            super(nIndexedColumns);
            this.indexPkPosition = new int[nIndexedColumns];
        }
        
        @Override
        protected int getIndexPkPosition(int dataPkPosition) {
            return this.indexPkPosition[dataPkPosition];
        }

        @Override
        protected int setIndexPkPosition(int dataPkPosition, int indexPkPosition) {
            return this.indexPkPosition[dataPkPosition] = indexPkPosition;
        }
        
        @Override
        public void write(DataOutput output) throws IOException {
            super.write(output);
            for (int i = 0; i < indexPkPosition.length; i++) {
                output.writeInt(indexPkPosition[i]);
            }
        }

        @Override
        protected int getByteSize() {
            return super.getByteSize() + indexPkPosition.length * Bytes.SIZEOF_INT;
        }

        @Override
        public void readFields(DataInput input) throws IOException {
            super.readFields(input);
            this.indexPkPosition = new int[getIndexPkColumnCount()];
            for (int i = 0; i < indexPkPosition.length; i++) {
                indexPkPosition[i] = input.readInt();
            }
        }
    }

    @Override
    public Iterator<ColumnReference> iterator() {
        return allColumns.iterator();
    }

    public ValueGetter createGetterFromKeyValues(final byte[] rowKey, Collection<? extends Cell> pendingUpdates) {
        final Map<ColumnReference, ImmutableBytesPtr> valueMap = Maps.newHashMapWithExpectedSize(pendingUpdates
                .size());
        for (Cell kv : pendingUpdates) {
            // create new pointers to each part of the kv
            ImmutableBytesPtr value = new ImmutableBytesPtr(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength());
            valueMap.put(new ColumnReference(kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength(), kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength()), value);
        }
        return new ValueGetter() {
            @Override
            public ImmutableBytesWritable getLatestValue(ColumnReference ref) {
                if(ref.equals(dataEmptyKeyValueRef)) return null;
                return valueMap.get(ref);
            }
            @Override
            public byte[] getRowKey() {
            	return rowKey;
            }
        };
    }

    public byte[] getDataEmptyKeyValueCF() {
        return dataEmptyKeyValueCF;
    }
    
    public boolean isLocalIndex() {
        return isLocalIndex;
    }
    
    public boolean isImmutableRows() {
        return immutableRows;
    }
    
    public Set<ColumnReference> getIndexedColumns() {
        return indexedColumns;
    }

    public static class UDFParseNodeVisitor extends StatelessTraverseAllParseNodeVisitor {

        private Map<String, UDFParseNode> udfParseNodes;
        public UDFParseNodeVisitor() {
            udfParseNodes = new HashMap<String, UDFParseNode>(1);
        }

        @Override
        public boolean visitEnter(FunctionParseNode node) throws SQLException {
            if(node instanceof UDFParseNode) {
                udfParseNodes.put(node.getName(), (UDFParseNode)node);
            }
            return super.visitEnter(node);
        }
        
        public Map<String, UDFParseNode> getUdfParseNodes() {
            return udfParseNodes;
        }
    }
    
    public byte[] getEmptyKeyValueQualifier() {
        return dataEmptyKeyValueRef.getQualifier();
    }
    
    public Set<Pair<String, String>> getIndexedColumnInfo() {
        return indexedColumnsInfo;
    }
    
    public ImmutableStorageScheme getIndexStorageScheme() {
        return immutableStorageScheme;
    }
    
}
