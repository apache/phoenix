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
package org.apache.phoenix.compile;

import static org.apache.phoenix.thirdparty.com.google.common.base.Preconditions.checkArgument;
import static org.apache.phoenix.thirdparty.com.google.common.collect.Lists.newArrayListWithCapacity;

import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.phoenix.index.PhoenixIndexBuilderHelper;
import org.apache.phoenix.schema.MaxPhoenixColumnSizeExceededException;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.cache.ServerCacheClient;
import org.apache.phoenix.compile.ExplainPlanAttributes
    .ExplainPlanAttributesBuilder;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.coprocessorclient.UngroupedAggregateRegionObserverHelper;
import org.apache.phoenix.exception.DataExceedsCapacityException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.AggregatePlan;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.execute.MutationState.MultiRowMutationState;
import org.apache.phoenix.execute.MutationState.RowMutationState;
import org.apache.phoenix.execute.MutationState.RowTimestampColInfo;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import org.apache.phoenix.optimize.QueryOptimizer;
import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.parse.BindParseNode;
import org.apache.phoenix.parse.ColumnName;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.parse.LiteralParseNode;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.SequenceValueParseNode;
import org.apache.phoenix.parse.UpsertStatement;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.ConstraintViolationException;
import org.apache.phoenix.schema.DelegateColumn;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTable.ViewType;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.ReadOnlyTableException;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.UpsertColumnsValuesMismatchException;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PUnsignedLong;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.ExpressionUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.thirdparty.com.google.common.collect.Sets;

public class UpsertCompiler {

    private static void setValues(byte[][] values, int[] pkSlotIndex, int[] columnIndexes,
            PTable table, MultiRowMutationState mutation, PhoenixStatement statement, boolean useServerTimestamp,
            IndexMaintainer maintainer, byte[][] viewConstants, byte[] onDupKeyBytes, int numSplColumns,
            int maxHBaseClientKeyValueSize) throws SQLException {
        long columnValueSize = 0;
        Map<PColumn,byte[]> columnValues = Maps.newHashMapWithExpectedSize(columnIndexes.length);
        byte[][] pkValues = new byte[table.getPKColumns().size()][];
        // If the table uses salting, the first byte is the salting byte, set to an empty array
        // here and we will fill in the byte later in PRowImpl.
        if (table.getBucketNum() != null) {
            pkValues[0] = new byte[] {0};
        }
        for(int i = 0; i < numSplColumns; i++) {
            pkValues[i + (table.getBucketNum() != null ? 1 : 0)] = values[i];
        }
        Long rowTimestamp = null; // case when the table doesn't have a row timestamp column
        RowTimestampColInfo rowTsColInfo = new RowTimestampColInfo(useServerTimestamp, rowTimestamp);
        for (int i = 0, j = numSplColumns; j < values.length; j++, i++) {
            byte[] value = values[j];
            PColumn column = table.getColumns().get(columnIndexes[i]);
            if (value.length >= maxHBaseClientKeyValueSize &&
                    table.getImmutableStorageScheme() == PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN) {
                String columnInfo = getExceedMaxHBaseClientKeyValueAllowanceColumnInfo(table, column.getName().getString());
                throw new MaxPhoenixColumnSizeExceededException(columnInfo, maxHBaseClientKeyValueSize, value.length);
            }

            if (SchemaUtil.isPKColumn(column)) {
                pkValues[pkSlotIndex[i]] = value;
                if (SchemaUtil.getPKPosition(table, column) == table.getRowTimestampColPos()) {
                    if (!useServerTimestamp) {
                        PColumn rowTimestampCol = table.getPKColumns().get(table.getRowTimestampColPos());
                        rowTimestamp = PLong.INSTANCE.getCodec().decodeLong(value, 0, rowTimestampCol.getSortOrder());
                        if (rowTimestamp < 0) {
                            throw new IllegalDataException("Value of a column designated as ROW_TIMESTAMP cannot be less than zero");
                        }
                        rowTsColInfo = new RowTimestampColInfo(useServerTimestamp, rowTimestamp);
                    } 
                }
            } else {
                columnValues.put(column, value);
                columnValueSize += (column.getEstimatedSize() + value.length);
            }
        }
        ImmutableBytesPtr ptr = new ImmutableBytesPtr();
        table.newKey(ptr, pkValues);
        if (table.getIndexType() == IndexType.LOCAL && maintainer != null) {
            byte[] rowKey = maintainer.buildDataRowKey(ptr, viewConstants);
            HRegionLocation region =
                    statement.getConnection().getQueryServices()
                            .getTableRegionLocation(table.getParentName().getBytes(), rowKey);
            byte[] regionPrefix =
                    region.getRegion().getStartKey().length == 0 ? new byte[region
                            .getRegion().getEndKey().length] : region.getRegion()
                            .getStartKey();
            if (regionPrefix.length != 0) {
                ptr.set(ScanRanges.prefixKey(ptr.get(), 0, ptr.getLength(), regionPrefix,
                    regionPrefix.length));
            }
        } 
        mutation.put(ptr, new RowMutationState(columnValues, columnValueSize, statement.getConnection().getStatementExecutionCounter(), rowTsColInfo, onDupKeyBytes));
    }

    public static String getExceedMaxHBaseClientKeyValueAllowanceColumnInfo(PTable table, String columnName) {
        return String.format("Upsert data to table %s on Column %s exceed max HBase client keyvalue size allowance",
                SchemaUtil.getTableName(table.getSchemaName().toString(), table.getTableName().toString()),
                columnName);
    }

    public static MutationState upsertSelect(StatementContext childContext, TableRef tableRef,
            RowProjector projector, ResultIterator iterator, int[] columnIndexes,
            int[] pkSlotIndexes, boolean useServerTimestamp,
            boolean prefixSysColValues) throws SQLException {
        PhoenixStatement statement = childContext.getStatement();
        PhoenixConnection connection = statement.getConnection();
        ConnectionQueryServices services = connection.getQueryServices();
        int maxSize = services.getProps().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB,
                QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
        long maxSizeBytes =
                services.getProps().getLongBytes(QueryServices.MAX_MUTATION_SIZE_BYTES_ATTRIB,
                    QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE_BYTES);
        int maxHBaseClientKeyValueSize =
                services.getProps().getInt(QueryServices.HBASE_CLIENT_KEYVALUE_MAXSIZE,
                        QueryServicesOptions.DEFAULT_HBASE_CLIENT_KEYVALUE_MAXSIZE);
        int batchSize = Math.min(connection.getMutateBatchSize(), maxSize);
        // we automatically flush the mutations when either auto commit is enabled, or
        // the target table is transactional (in that case changes are not visible until we commit)
        final boolean autoFlush = connection.getAutoCommit() || tableRef.getTable().isTransactional();
        int sizeOffset = 0;
        int numSplColumns =
                (tableRef.getTable().isMultiTenant() ? 1 : 0)
                        + (tableRef.getTable().getViewIndexId() != null ? 1 : 0);
        byte[][] values = new byte[columnIndexes.length + numSplColumns][];
        if(prefixSysColValues) {
            int i = 0;
            if(tableRef.getTable().isMultiTenant()) {
                values[i++] = connection.getTenantId().getBytes();
            }
            if(tableRef.getTable().getViewIndexId() != null) {
                values[i++] = PSmallint.INSTANCE.toBytes(tableRef.getTable().getViewIndexId());
            }
        }
        int rowCount = 0;
        MultiRowMutationState mutation = new MultiRowMutationState(batchSize);
        PTable table = tableRef.getTable();
        IndexMaintainer indexMaintainer = null;
        byte[][] viewConstants = null;
        if (table.getIndexType() == IndexType.LOCAL) {
            PTable parentTable =
                    statement
                            .getConnection()
                            .getMetaDataCache()
                            .getTableRef(
                                new PTableKey(statement.getConnection().getTenantId(), table
                                        .getParentName().getString())).getTable();
            indexMaintainer = table.getIndexMaintainer(parentTable, connection);
            viewConstants = IndexUtil.getViewConstants(parentTable);
        }
        try (ResultSet rs = new PhoenixResultSet(iterator, projector, childContext)) {
            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            while (rs.next()) {
                for (int i = 0, j = numSplColumns; j < values.length; j++, i++) {
                    PColumn column = table.getColumns().get(columnIndexes[i]);
                    byte[] bytes = rs.getBytes(i + 1);
                    ptr.set(bytes == null ? ByteUtil.EMPTY_BYTE_ARRAY : bytes);
                    Object value = rs.getObject(i + 1);
                    int rsPrecision = rs.getMetaData().getPrecision(i + 1);
                    Integer precision = rsPrecision == 0 ? null : rsPrecision;
                    int rsScale = rs.getMetaData().getScale(i + 1);
                    Integer scale = rsScale == 0 ? null : rsScale;
                    // We are guaranteed that the two column will have compatible types,
                    // as we checked that before.
                    if (!column.getDataType().isSizeCompatible(ptr, value, column.getDataType(),
                            SortOrder.getDefault(), precision,
                            scale, column.getMaxLength(), column.getScale())) {
                        throw new DataExceedsCapacityException(column.getDataType(), column.getMaxLength(),
                                column.getScale(), column.getName().getString());
                    }
                    column.getDataType().coerceBytes(ptr, value, column.getDataType(), 
                            precision, scale, SortOrder.getDefault(), 
                            column.getMaxLength(), column.getScale(), column.getSortOrder(),
                            table.rowKeyOrderOptimizable());
                    values[j] = ByteUtil.copyKeyBytesIfNecessary(ptr);
                }
                setValues(values, pkSlotIndexes, columnIndexes, table, mutation, statement,
                        useServerTimestamp, indexMaintainer, viewConstants, null,
                        numSplColumns, maxHBaseClientKeyValueSize);
                rowCount++;
                // Commit a batch if auto commit is true and we're at our batch size
                if (autoFlush && rowCount % batchSize == 0) {
                    MutationState state = new MutationState(tableRef, mutation, 0,
                            maxSize, maxSizeBytes, connection);
                    connection.getMutationState().join(state);
                    connection.getMutationState().send();
                    mutation.clear();
                }
            }

            if (autoFlush) {
                // If auto commit is true, this last batch will be committed upon return
                sizeOffset = rowCount / batchSize * batchSize;
            }
            return new MutationState(tableRef, mutation, sizeOffset, maxSize,
                    maxSizeBytes, connection);
        }
    }

    private static class UpsertingParallelIteratorFactory extends MutatingParallelIteratorFactory {
        private RowProjector projector;
        private int[] columnIndexes;
        private int[] pkSlotIndexes;
        private final TableRef tableRef;
        private final boolean useSeverTimestamp;

        private UpsertingParallelIteratorFactory (PhoenixConnection connection, TableRef tableRef, boolean useServerTimestamp) {
            super(connection);
            this.tableRef = tableRef;
            this.useSeverTimestamp = useServerTimestamp;
        }

        @Override
        protected MutationState mutate(StatementContext parentContext, ResultIterator iterator, PhoenixConnection connection) throws SQLException {
            if (parentContext.getSequenceManager().getSequenceCount() > 0) {
                throw new IllegalStateException("Cannot pipeline upsert when sequence is referenced");
            }
            PhoenixStatement statement = new PhoenixStatement(connection);
            /*
             * We don't want to collect any read metrics within the child context. This is because any read metrics that
             * need to be captured are already getting collected in the parent statement context enclosed in the result
             * iterator being used for reading rows out.
             */
            StatementContext childContext = new StatementContext(statement, false);
            // Clone the row projector as it's not thread safe and would be used simultaneously by
            // multiple threads otherwise.
            return upsertSelect(childContext, tableRef, projector.cloneIfNecessary(), iterator,
                    columnIndexes, pkSlotIndexes, useSeverTimestamp, false);
        }
        
        public void setRowProjector(RowProjector projector) {
            this.projector = projector;
        }
        public void setColumnIndexes(int[] columnIndexes) {
            this.columnIndexes = columnIndexes;
        }
        public void setPkSlotIndexes(int[] pkSlotIndexes) {
            this.pkSlotIndexes = pkSlotIndexes;
        }
    }
    
    private final PhoenixStatement statement;
    private final Operation operation;
    
    public UpsertCompiler(PhoenixStatement statement, Operation operation) {
        this.statement = statement;
        this.operation = operation;
    }
    
    private static LiteralParseNode getNodeForRowTimestampColumn(PColumn col) {
        PDataType type = col.getDataType();
        long dummyValue = 0L;
        if (type.isCoercibleTo(PTimestamp.INSTANCE)) {
            return new LiteralParseNode(new Timestamp(dummyValue), PTimestamp.INSTANCE);
        } else if (type == PLong.INSTANCE || type == PUnsignedLong.INSTANCE) {
            return new LiteralParseNode(dummyValue, PLong.INSTANCE);
        }
        throw new IllegalArgumentException();
    }
    
    public MutationPlan compile(UpsertStatement upsert) throws SQLException {
        final PhoenixConnection connection = statement.getConnection();
        ConnectionQueryServices services = connection.getQueryServices();
        final int maxSize = services.getProps().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB,QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
        final long maxSizeBytes = services.getProps()
                .getLongBytes(QueryServices.MAX_MUTATION_SIZE_BYTES_ATTRIB,
                        QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE_BYTES);
        List<ColumnName> columnNodes = upsert.getColumns();
        TableRef tableRefToBe = null;
        PTable table = null;
        Set<PColumn> addViewColumnsToBe = Collections.emptySet();
        Set<PColumn> overlapViewColumnsToBe = Collections.emptySet();
        List<PColumn> allColumnsToBe = Collections.emptyList();
        boolean isTenantSpecific = false;
        boolean isSharedViewIndex = false;
        String tenantIdStr = null;
        ColumnResolver resolver = null;
        int[] columnIndexesToBe;
        int nColumnsToSet = 0;
        int[] pkSlotIndexesToBe;
        List<ParseNode> valueNodes = upsert.getValues();
        List<PColumn> targetColumns;
        NamedTableNode tableNode = upsert.getTable();
        String tableName = tableNode.getName().getTableName();
        String schemaName = tableNode.getName().getSchemaName();
        QueryPlan queryPlanToBe = null;
        int nValuesToSet;
        boolean sameTable = false;
        boolean runOnServer = false;
        boolean serverUpsertSelectEnabled =
                services.getProps().getBoolean(QueryServices.ENABLE_SERVER_UPSERT_SELECT,
                        QueryServicesOptions.DEFAULT_ENABLE_SERVER_UPSERT_SELECT);
        boolean allowServerMutations =
                services.getProps().getBoolean(QueryServices.ENABLE_SERVER_SIDE_UPSERT_MUTATIONS,
                        QueryServicesOptions.DEFAULT_ENABLE_SERVER_SIDE_UPSERT_MUTATIONS);
        UpsertingParallelIteratorFactory parallelIteratorFactoryToBe = null;
        boolean useServerTimestampToBe = false;
        

        resolver = FromCompiler.getResolverForMutation(upsert, connection);
        tableRefToBe = resolver.getTables().get(0);
        table = tableRefToBe.getTable();
        // Cannot update:
        // - read-only VIEW
        // - transactional table with a connection having an SCN
        // - table with indexes and SCN set
        // - tables with ROW_TIMESTAMP columns
        if (table.getType() == PTableType.VIEW && table.getViewType().isReadOnly()) {
            throw new ReadOnlyTableException(schemaName,tableName);
        } else if (connection.isBuildingIndex() && table.getType() != PTableType.INDEX) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.ONLY_INDEX_UPDATABLE_AT_SCN)
            .setSchemaName(schemaName)
            .setTableName(tableName)
            .build().buildException();
        } else if (table.isTransactional() && connection.getSCN() != null) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode
                    .CANNOT_SPECIFY_SCN_FOR_TXN_TABLE)
                    .setSchemaName(schemaName)
                    .setTableName(tableName).build().buildException();
        } else if (connection.getSCN() != null && !table.getIndexes().isEmpty()
                && !connection.isRunningUpgrade() && !connection.isBuildingIndex()) {
            throw new SQLExceptionInfo
                    .Builder(SQLExceptionCode
                    .CANNOT_UPSERT_WITH_SCN_FOR_TABLE_WITH_INDEXES)
                    .setSchemaName(schemaName)
                    .setTableName(tableName).build().buildException();
        } else if(connection.getSCN() != null && !connection.isRunningUpgrade()
                && !connection.isBuildingIndex() && table.getRowTimestampColPos() >= 0) {
            throw new SQLExceptionInfo
                    .Builder(SQLExceptionCode
                    .CANNOT_UPSERT_WITH_SCN_FOR_ROW_TIMESTAMP_COLUMN)
                    .setSchemaName(schemaName)
                    .setTableName(tableName).build().buildException();
        }
        boolean isSalted = table.getBucketNum() != null;
        isTenantSpecific = table.isMultiTenant() && connection.getTenantId() != null;
        isSharedViewIndex = table.getViewIndexId() != null;
        tenantIdStr = isTenantSpecific ? connection.getTenantId().getString() : null;
        int posOffset = isSalted ? 1 : 0;
        // Setup array of column indexes parallel to values that are going to be set
        allColumnsToBe = table.getColumns();

        nColumnsToSet = 0;
        if (table.getViewType() == ViewType.UPDATABLE) {
            addViewColumnsToBe = Sets.newLinkedHashSetWithExpectedSize(allColumnsToBe.size());
            for (PColumn column : allColumnsToBe) {
                if (column.getViewConstant() != null) {
                    addViewColumnsToBe.add(column);
                }
            }
        }
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        // Allow full row upsert if no columns or only dynamic ones are specified and values count match
        if (columnNodes.isEmpty() || columnNodes.size() == upsert.getTable().getDynamicColumns().size()) {
            nColumnsToSet = allColumnsToBe.size() - posOffset;
            columnIndexesToBe = new int[nColumnsToSet];
            pkSlotIndexesToBe = new int[columnIndexesToBe.length];
            targetColumns = Lists.newArrayListWithExpectedSize(columnIndexesToBe.length);
            targetColumns.addAll(Collections.<PColumn>nCopies(columnIndexesToBe.length, null));
            int minPKPos = 0;
            if (isSharedViewIndex) {
                PColumn indexIdColumn = table.getPKColumns().get(minPKPos);
                columnIndexesToBe[minPKPos] = indexIdColumn.getPosition();
                targetColumns.set(minPKPos, indexIdColumn);
                minPKPos++;
            }
            if (isTenantSpecific) {
                PColumn tenantColumn = table.getPKColumns().get(minPKPos);
                columnIndexesToBe[minPKPos] = tenantColumn.getPosition();
                targetColumns.set(minPKPos, tenantColumn);
                minPKPos++;
            }
            for (int i = posOffset, j = 0; i < allColumnsToBe.size(); i++) {
                PColumn column = allColumnsToBe.get(i);
                if (SchemaUtil.isPKColumn(column)) {
                    pkSlotIndexesToBe[i-posOffset] = j + posOffset;
                    if (j++ < minPKPos) { // Skip, as it's already been set above
                        continue;
                    }
                    minPKPos = 0;
                }
                columnIndexesToBe[i-posOffset+minPKPos] = i;
                targetColumns.set(i-posOffset+minPKPos, column);
            }
            if (!addViewColumnsToBe.isEmpty()) {
                // All view columns overlap in this case
                overlapViewColumnsToBe = addViewColumnsToBe;
                addViewColumnsToBe = Collections.emptySet();
            }
        } else {
            // Size for worse case
            int numColsInUpsert = columnNodes.size();
            nColumnsToSet = numColsInUpsert + addViewColumnsToBe.size() + (isTenantSpecific ? 1 : 0) +  + (isSharedViewIndex ? 1 : 0);
            columnIndexesToBe = new int[nColumnsToSet];
            pkSlotIndexesToBe = new int[columnIndexesToBe.length];
            targetColumns = Lists.newArrayListWithExpectedSize(columnIndexesToBe.length);
            targetColumns.addAll(Collections.<PColumn>nCopies(columnIndexesToBe.length, null));
            Arrays.fill(columnIndexesToBe, -1); // TODO: necessary? So we'll get an AIOB exception if it's not replaced
            Arrays.fill(pkSlotIndexesToBe, -1); // TODO: necessary? So we'll get an AIOB exception if it's not replaced
            BitSet columnsBeingSet = new BitSet(table.getColumns().size());
            int i = 0;
            if (isSharedViewIndex) {
                PColumn indexIdColumn = table.getPKColumns().get(i + posOffset);
                columnsBeingSet.set(columnIndexesToBe[i] = indexIdColumn.getPosition());
                pkSlotIndexesToBe[i] = i + posOffset;
                targetColumns.set(i, indexIdColumn);
                i++;
            }
            // Add tenant column directly, as we don't want to resolve it as this will fail
            if (isTenantSpecific) {
                PColumn tenantColumn = table.getPKColumns().get(i + posOffset);
                columnsBeingSet.set(columnIndexesToBe[i] = tenantColumn.getPosition());
                pkSlotIndexesToBe[i] = i + posOffset;
                targetColumns.set(i, tenantColumn);
                i++;
            }
            for (ColumnName colName : columnNodes) {
                ColumnRef ref = resolver.resolveColumn(null, colName.getFamilyName(), colName.getColumnName());
                PColumn column = ref.getColumn();
                if (IndexUtil.getViewConstantValue(column, ptr)) {
                    if (overlapViewColumnsToBe.isEmpty()) {
                        overlapViewColumnsToBe = Sets.newHashSetWithExpectedSize(addViewColumnsToBe.size());
                    }
                    nColumnsToSet--;
                    overlapViewColumnsToBe.add(column);
                    addViewColumnsToBe.remove(column);
                }
                columnsBeingSet.set(columnIndexesToBe[i] = ref.getColumnPosition());
                targetColumns.set(i, column);
                if (SchemaUtil.isPKColumn(column)) {
                    pkSlotIndexesToBe[i] = ref.getPKSlotPosition();
                }
                i++;
            }
            for (PColumn column : addViewColumnsToBe) {
                columnsBeingSet.set(columnIndexesToBe[i] = column.getPosition());
                targetColumns.set(i, column);
                if (SchemaUtil.isPKColumn(column)) {
                    pkSlotIndexesToBe[i] = SchemaUtil.getPKPosition(table, column);
                }
                i++;
            }
            // If a table has rowtimestamp col, then we always set it.
            useServerTimestampToBe = table.getRowTimestampColPos() != -1 && !isRowTimestampSet(pkSlotIndexesToBe, table);
            if (useServerTimestampToBe) {
                PColumn rowTimestampCol = table.getPKColumns().get(table.getRowTimestampColPos());
                // Need to resize columnIndexesToBe and pkSlotIndexesToBe to include this extra column.
                columnIndexesToBe = Arrays.copyOf(columnIndexesToBe, columnIndexesToBe.length + 1);
                pkSlotIndexesToBe = Arrays.copyOf(pkSlotIndexesToBe, pkSlotIndexesToBe.length + 1);
                columnsBeingSet.set(columnIndexesToBe[i] = rowTimestampCol.getPosition());
                pkSlotIndexesToBe[i] = table.getRowTimestampColPos();
                targetColumns.add(rowTimestampCol);
                if (valueNodes != null && !valueNodes.isEmpty()) {
                    valueNodes.add(getNodeForRowTimestampColumn(rowTimestampCol));
                }
                nColumnsToSet++;
            }
            for (i = posOffset; i < table.getColumns().size(); i++) {
                PColumn column = table.getColumns().get(i);
                if (!columnsBeingSet.get(i) && !column.isNullable() && column.getExpressionStr() == null) {
                    throw new ConstraintViolationException(table.getName().getString() + "."
                            + SchemaUtil.getColumnDisplayName(column) + " may not be null");
                }
            }
        }
        boolean isAutoCommit = connection.getAutoCommit();
        if (valueNodes == null) {
            SelectStatement select = upsert.getSelect();
            assert(select != null);
            select = SubselectRewriter.flatten(select, connection);
            ColumnResolver selectResolver = FromCompiler.getResolverForQuery(select, connection, false, upsert.getTable().getName());
            select = StatementNormalizer.normalize(select, selectResolver);
            select = prependTenantAndViewConstants(table, select, tenantIdStr, addViewColumnsToBe, useServerTimestampToBe);
            SelectStatement transformedSelect = SubqueryRewriter.transform(select, selectResolver, connection);
            if (transformedSelect != select) {
                selectResolver = FromCompiler.getResolverForQuery(transformedSelect, connection, false, upsert.getTable().getName());
                select = StatementNormalizer.normalize(transformedSelect, selectResolver);
            }
            sameTable = !select.isJoin()
                && tableRefToBe.equals(selectResolver.getTables().get(0));
            /* We can run the upsert in a coprocessor if:
             * 1) from has only 1 table or server UPSERT SELECT is enabled
             * 2) the select query isn't doing aggregation (which requires a client-side final merge)
             * 3) autoCommit is on
             * 4) the table is not immutable with indexes, as the client is the one that figures out the additional
             *    puts for index tables.
             * 5) no limit clause, as the limit clause requires client-side post processing
             * 6) no sequences, as sequences imply that the order of upsert must match the order of
             *    selection. TODO: change this and only force client side if there's a ORDER BY on the sequence value
             * Otherwise, run the query to pull the data from the server
             * and populate the MutationState (upto a limit).
            */
            if (! (select.isAggregate() || select.isDistinct() || select.getLimit() != null || select.hasSequence()) ) {
                // We can pipeline the upsert select instead of spooling everything to disk first,
                // if we don't have any post processing that's required.
                parallelIteratorFactoryToBe = new UpsertingParallelIteratorFactory(connection, tableRefToBe, useServerTimestampToBe);
                // If we're in the else, then it's not an aggregate, distinct, limited, or sequence using query,
                // so we might be able to run it entirely on the server side.
                // region space managed by region servers. So we bail out on executing on server side.
                // Disable running upsert select on server side if a table has global mutable secondary indexes on it
                boolean hasGlobalMutableIndexes = SchemaUtil.hasGlobalIndex(table) && !table.isImmutableRows();
                boolean hasWhereSubquery = select.getWhere() != null && select.getWhere().hasSubquery();
                runOnServer = (sameTable || (serverUpsertSelectEnabled && !hasGlobalMutableIndexes)) && isAutoCommit 
                        // We can run the upsert select for initial index population on server side for transactional
                        // tables since the writes do not need to be done transactionally, since we gate the index
                        // usage on successfully writing all data rows.
                        && (!table.isTransactional() || table.getType() == PTableType.INDEX)
                        && !(table.isImmutableRows() && !table.getIndexes().isEmpty())
                        && !select.isJoin() && !hasWhereSubquery && table.getRowTimestampColPos() == -1;
            }
            runOnServer &= allowServerMutations;
            // If we may be able to run on the server, add a hint that favors using the data table
            // if all else is equal.
            // TODO: it'd be nice if we could figure out in advance if the PK is potentially changing,
            // as this would disallow running on the server. We currently use the row projector we
            // get back to figure this out.
            HintNode hint = upsert.getHint();
            if (!upsert.getHint().hasHint(Hint.USE_INDEX_OVER_DATA_TABLE)) {
                hint = HintNode.create(hint, Hint.USE_DATA_OVER_INDEX_TABLE);
            }
            select = SelectStatement.create(select, hint);
            // Pass scan through if same table in upsert and select so that projection is computed correctly
            // Use optimizer to choose the best plan
            QueryCompiler compiler = new QueryCompiler(statement, select, selectResolver, targetColumns, parallelIteratorFactoryToBe, new SequenceManager(statement), true, false, null);
            queryPlanToBe = compiler.compile();

            if (sameTable) {
                // in the UPSERT INTO X ... SELECT FROM X case enforce the source tableRef's TS
                // as max TS, so that the query can safely restarted and still work of a snapshot
                // (so it won't see its own data in case of concurrent splits)
                // see PHOENIX-4849
                long serverTime = selectResolver.getTables().get(0).getCurrentTime();
                if (serverTime == QueryConstants.UNSET_TIMESTAMP) {
                    // if this is the first time this table is resolved the ref's current time might not be defined, yet
                    // in that case force an RPC to get the server time
                    serverTime = new MetaDataClient(connection).getCurrentTime(schemaName, tableName);
                }
                Scan scan = queryPlanToBe.getContext().getScan();
                ScanUtil.setTimeRange(scan, scan.getTimeRange().getMin(), serverTime);
            }
            // This is post-fix: if the tableRef is a projected table, this means there are post-processing
            // steps and parallelIteratorFactory did not take effect.
            if (queryPlanToBe.getTableRef().getTable().getType() == PTableType.PROJECTED || queryPlanToBe.getTableRef().getTable().getType() == PTableType.SUBQUERY) {
                parallelIteratorFactoryToBe = null;
            }
            nValuesToSet = queryPlanToBe.getProjector().getColumnCount();
            // Cannot auto commit if doing aggregation or topN or salted
            // Salted causes problems because the row may end up living on a different region
        } else {
            nValuesToSet = valueNodes.size() + addViewColumnsToBe.size() + (isTenantSpecific ? 1 : 0) + (isSharedViewIndex ? 1 : 0);
        }
        // Resize down to allow a subset of columns to be specifiable
        if (columnNodes.isEmpty() && columnIndexesToBe.length >= nValuesToSet) {
            nColumnsToSet = nValuesToSet;
            columnIndexesToBe = Arrays.copyOf(columnIndexesToBe, nValuesToSet);
            pkSlotIndexesToBe = Arrays.copyOf(pkSlotIndexesToBe, nValuesToSet);
            for (int i = posOffset + nValuesToSet; i < table.getColumns().size(); i++) {
                PColumn column = table.getColumns().get(i);
                if (!column.isNullable() && column.getExpressionStr() == null) {
                    throw new ConstraintViolationException(table.getName().getString() + "."
                            + SchemaUtil.getColumnDisplayName(column) + " may not be null");
                }
            }
        }
        
        if (nValuesToSet != nColumnsToSet) {
            // We might have added columns, so refresh cache and try again if stale.
            // We have logic to catch MetaNotFoundException and refresh cache  in PhoenixStatement
            // Note that this check is not really sufficient, as a column could have
            // been removed and the added back and we wouldn't detect that here.
            throw new UpsertColumnsValuesMismatchException(schemaName, tableName,
              "Numbers of columns: " + nColumnsToSet + ". Number of values: " + nValuesToSet);
        }
        final QueryPlan originalQueryPlan = queryPlanToBe;
        RowProjector projectorToBe = null;
        // Optimize only after all checks have been performed
        if (valueNodes == null) {
            queryPlanToBe = new QueryOptimizer(services).optimize(queryPlanToBe, statement, targetColumns, parallelIteratorFactoryToBe);
            projectorToBe = queryPlanToBe.getProjector();
        }
        final List<PColumn> allColumns = allColumnsToBe;
        final RowProjector projector = projectorToBe;
        final QueryPlan queryPlan = queryPlanToBe;
        final TableRef tableRef = tableRefToBe;
        final Set<PColumn> addViewColumns = addViewColumnsToBe;
        final Set<PColumn> overlapViewColumns = overlapViewColumnsToBe;
        final UpsertingParallelIteratorFactory parallelIteratorFactory = parallelIteratorFactoryToBe;
        final int[] columnIndexes = columnIndexesToBe;
        final int[] pkSlotIndexes = pkSlotIndexesToBe;
        final boolean useServerTimestamp = useServerTimestampToBe;
        if (table.getRowTimestampColPos() == -1 && useServerTimestamp) {
            throw new IllegalStateException("For a table without row timestamp column, useServerTimestamp cannot be true");
        }
        // TODO: break this up into multiple functions
        ////////////////////////////////////////////////////////////////////
        // UPSERT SELECT
        /////////////////////////////////////////////////////////////////////
        if (valueNodes == null) {
            // Before we re-order, check that for updatable view columns
            // the projected expression either matches the column name or
            // is a constant with the same required value.
            throwIfNotUpdatable(tableRef, overlapViewColumnsToBe, targetColumns, projector, sameTable);
            
            ////////////////////////////////////////////////////////////////////
            // UPSERT SELECT run server-side (maybe)
            /////////////////////////////////////////////////////////////////////
            if (runOnServer) {
                // At most this array will grow bigger by the number of PK columns
                int[] allColumnsIndexes = Arrays.copyOf(columnIndexes, columnIndexes.length + nValuesToSet);
                int[] reverseColumnIndexes = new int[table.getColumns().size()];
                List<Expression> projectedExpressions = Lists.newArrayListWithExpectedSize(reverseColumnIndexes.length);
                Arrays.fill(reverseColumnIndexes, -1);
                for (int i =0; i < nValuesToSet; i++) {
                    projectedExpressions.add(projector.getColumnProjector(i).getExpression());
                    reverseColumnIndexes[columnIndexes[i]] = i;
                }
                /*
                 * Order projected columns and projected expressions with PK columns
                 * leading order by slot position
                 */
                int offset = table.getBucketNum() == null ? 0 : 1;
                for (int i = 0; i < table.getPKColumns().size() - offset; i++) {
                    PColumn column = table.getPKColumns().get(i + offset);
                    int pos = reverseColumnIndexes[column.getPosition()];
                    if (pos == -1) {
                        // Last PK column may be fixed width and nullable
                        // We don't want to insert a null expression b/c
                        // it's not valid to set a fixed width type to null.
                        if (column.getDataType().isFixedWidth()) {
                            continue;
                        }
                        // Add literal null for missing PK columns
                        pos = projectedExpressions.size();
                        Expression literalNull = LiteralExpression.newConstant(null, column.getDataType(), Determinism.ALWAYS);
                        projectedExpressions.add(literalNull);
                        allColumnsIndexes[pos] = column.getPosition();
                    }
                    // Swap select expression at pos with i
                    Collections.swap(projectedExpressions, i, pos);
                    // Swap column indexes and reverse column indexes too
                    int tempPos = allColumnsIndexes[i];
                    allColumnsIndexes[i] = allColumnsIndexes[pos];
                    allColumnsIndexes[pos] = tempPos;
                    reverseColumnIndexes[tempPos] = pos;
                    reverseColumnIndexes[i] = i;
                }
                // If any pk slots are changing and server side UPSERT SELECT is disabled, do not run on server
                if (!serverUpsertSelectEnabled && ExpressionUtil
                        .isPkPositionChanging(new TableRef(table), projectedExpressions)) {
                    runOnServer = false;
                }
                ////////////////////////////////////////////////////////////////////
                // UPSERT SELECT run server-side
                /////////////////////////////////////////////////////////////////////
                if (runOnServer) {
                    // Iterate through columns being projected
                    List<PColumn> projectedColumns = Lists.newArrayListWithExpectedSize(projectedExpressions.size());
                    int posOff = table.getBucketNum() != null ? 1 : 0;
                    for (int i = 0 ; i < projectedExpressions.size(); i++) {
                        // Must make new column if position has changed
                        PColumn column = allColumns.get(allColumnsIndexes[i]);
                        projectedColumns.add(column.getPosition() == i + posOff ? column : new PColumnImpl(column, i + posOff));
                    }
                    // Build table from projectedColumns
                    // Hack to add default column family to be used on server in case no value column is projected.
                    PTable projectedTable = PTableImpl.builderWithColumns(table, projectedColumns)
                            .setExcludedColumns(ImmutableList.of())
                            .setDefaultFamilyName(PNameFactory.newName(SchemaUtil.getEmptyColumnFamily(table)))
                            .build();
                    
                    SelectStatement select = SelectStatement.create(SelectStatement.COUNT_ONE, upsert.getHint());
                    StatementContext statementContext = queryPlan.getContext();
                    RowProjector aggProjectorToBe = ProjectionCompiler.compile(statementContext, select, GroupBy
                            .EMPTY_GROUP_BY);
                    statementContext.getAggregationManager().compile(queryPlan.getContext()
                            ,GroupBy.EMPTY_GROUP_BY);
                    if (queryPlan.getProjector().projectEveryRow()) {
                        aggProjectorToBe = new RowProjector(aggProjectorToBe,true);
                    }
                    final RowProjector aggProjector = aggProjectorToBe;

                    /*
                     * Transfer over PTable representing subset of columns selected, but all PK columns.
                     * Move columns setting PK first in pkSlot order, adding LiteralExpression of null for any missing ones.
                     * Transfer over List<Expression> for projection.
                     * In region scan, evaluate expressions in order, collecting first n columns for PK and collection non PK in mutation Map
                     * Create the PRow and get the mutations, adding them to the batch
                     */
                    final StatementContext context = queryPlan.getContext();
                    final Scan scan = context.getScan();
                    scan.setAttribute(BaseScannerRegionObserverConstants.UPSERT_SELECT_TABLE, UngroupedAggregateRegionObserverHelper.serialize(projectedTable));
                    scan.setAttribute(BaseScannerRegionObserverConstants.UPSERT_SELECT_EXPRS, UngroupedAggregateRegionObserverHelper.serialize(projectedExpressions));
                    // Ignore order by - it has no impact
                    final QueryPlan aggPlan = new AggregatePlan(context, select, statementContext.getCurrentTable(), aggProjector, null,null, OrderBy.EMPTY_ORDER_BY, null, GroupBy.EMPTY_GROUP_BY, null, originalQueryPlan);
                    return new ServerUpsertSelectMutationPlan(queryPlan, tableRef, originalQueryPlan, context, connection, scan, aggPlan, aggProjector, maxSize, maxSizeBytes);
                }
            }
            ////////////////////////////////////////////////////////////////////
            // UPSERT SELECT run client-side
            /////////////////////////////////////////////////////////////////////
            return new ClientUpsertSelectMutationPlan(queryPlan, tableRef, originalQueryPlan, parallelIteratorFactory, projector, columnIndexes, pkSlotIndexes, useServerTimestamp, maxSize, maxSizeBytes);
        }

            
        ////////////////////////////////////////////////////////////////////
        // UPSERT VALUES
        /////////////////////////////////////////////////////////////////////
        final byte[][] values = new byte[nValuesToSet][];
        int nodeIndex = 0;
        if (isSharedViewIndex) {
            values[nodeIndex++] = table.getviewIndexIdType().toBytes(table.getViewIndexId());
        }
        if (isTenantSpecific) {
            PName tenantId = connection.getTenantId();
            values[nodeIndex++] = ScanUtil.getTenantIdBytes(table.getRowKeySchema(), table.getBucketNum() != null, tenantId, isSharedViewIndex);
        }
        
        final int nodeIndexOffset = nodeIndex;
        // Allocate array based on size of all columns in table,
        // since some values may not be set (if they're nullable).
        final StatementContext context = new StatementContext(statement, resolver, new Scan(), new SequenceManager(statement));
        UpsertValuesCompiler expressionBuilder = new UpsertValuesCompiler(context);
        final List<Expression> constantExpressions = Lists.newArrayListWithExpectedSize(valueNodes.size());
        // First build all the expressions, as with sequences we want to collect them all first
        // and initialize them in one batch
        for (ParseNode valueNode : valueNodes) {
            if (!valueNode.isStateless()) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.VALUE_IN_UPSERT_NOT_CONSTANT).build().buildException();
            }
            PColumn column = allColumns.get(columnIndexes[nodeIndex]);
            expressionBuilder.setColumn(column);
            Expression expression = valueNode.accept(expressionBuilder);
            if (expression.getDataType() != null && !expression.getDataType().isCastableTo(column.getDataType())) {
                throw TypeMismatchException.newException(
                        expression.getDataType(), column.getDataType(), "expression: "
                                + expression.toString() + " in column " + column);
            }
            constantExpressions.add(expression);
            nodeIndex++;
        }
        byte[] onDupKeyBytesToBe = null;
        List<Pair<ColumnName,ParseNode>> onDupKeyPairs = upsert.getOnDupKeyPairs();
        if (onDupKeyPairs != null) {
            if (table.isImmutableRows()) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_USE_ON_DUP_KEY_FOR_IMMUTABLE)
                .setSchemaName(table.getSchemaName().getString())
                .setTableName(table.getTableName().getString())
                .build().buildException();
            }
            if (table.isTransactional()) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_USE_ON_DUP_KEY_FOR_TRANSACTIONAL)
                .setSchemaName(table.getSchemaName().getString())
                .setTableName(table.getTableName().getString())
                .build().buildException();
            }
            if (connection.getSCN() != null) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_SET_SCN_IN_ON_DUP_KEY)
                .setSchemaName(table.getSchemaName().getString())
                .setTableName(table.getTableName().getString())
                .build().buildException();
            }
            if (onDupKeyPairs.isEmpty()) { // ON DUPLICATE KEY IGNORE
                onDupKeyBytesToBe = PhoenixIndexBuilderHelper.serializeOnDupKeyIgnore();
            } else {                       // ON DUPLICATE KEY UPDATE;
                int position = table.getBucketNum() == null ? 0 : 1;
                UpdateColumnCompiler compiler = new UpdateColumnCompiler(context);
                int nColumns = onDupKeyPairs.size();
                List<Expression> updateExpressions = Lists.newArrayListWithExpectedSize(nColumns);
                LinkedHashSet<PColumn> updateColumns = Sets.newLinkedHashSetWithExpectedSize(nColumns + 1);
                updateColumns.add(new PColumnImpl(
                        table.getPKColumns().get(position).getName(), // Use first PK column name as we know it won't conflict with others
                        null, PVarbinary.INSTANCE, null, null, false, position, SortOrder.getDefault(), 0, null, false, null, false, false, null, table.getPKColumns().get(position).getTimestamp()));
                position++;
                for (Pair<ColumnName,ParseNode> columnPair : onDupKeyPairs) {
                    ColumnName colName = columnPair.getFirst();
                    PColumn updateColumn = resolver.resolveColumn(null, colName.getFamilyName(), colName.getColumnName()).getColumn();
                    if (SchemaUtil.isPKColumn(updateColumn)) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_UPDATE_PK_ON_DUP_KEY)
                        .setSchemaName(table.getSchemaName().getString())
                        .setTableName(table.getTableName().getString())
                        .setColumnName(updateColumn.getName().getString())
                        .build().buildException();
                    }
                    final int columnPosition = position++;
                    if (!updateColumns.add(new DelegateColumn(updateColumn) {
                        @Override
                        public int getPosition() {
                            return columnPosition;
                        }
                    })) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.DUPLICATE_COLUMN_IN_ON_DUP_KEY)
                            .setSchemaName(table.getSchemaName().getString())
                            .setTableName(table.getTableName().getString())
                            .setColumnName(updateColumn.getName().getString())
                            .build().buildException();
                    };
                    ParseNode updateNode = columnPair.getSecond();
                    compiler.setColumn(updateColumn);
                    Expression updateExpression = updateNode.accept(compiler);
                    // Check that updateExpression is coercible to updateColumn
                    if (updateExpression.getDataType() != null && !updateExpression.getDataType().isCastableTo(updateColumn.getDataType())) {
                        throw TypeMismatchException.newException(
                                updateExpression.getDataType(), updateColumn.getDataType(), "expression: "
                                        + updateExpression.toString() + " for column " + updateColumn);
                    }
                    if (compiler.isAggregate()) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.AGGREGATION_NOT_ALLOWED_IN_ON_DUP_KEY)
                            .setSchemaName(table.getSchemaName().getString())
                            .setTableName(table.getTableName().getString())
                            .setColumnName(updateColumn.getName().getString())
                            .build().buildException();
                    }
                    updateExpressions.add(updateExpression);
                }
                PTable onDupKeyTable = PTableImpl.builderWithColumns(table, updateColumns)
                        .build();
                onDupKeyBytesToBe = PhoenixIndexBuilderHelper.serializeOnDupKeyUpdate(onDupKeyTable, updateExpressions);
            }
        }
        final byte[] onDupKeyBytes = onDupKeyBytesToBe;
        
        return new UpsertValuesMutationPlan(context, tableRef, nodeIndexOffset, constantExpressions,
                allColumns, columnIndexes, overlapViewColumns, values, addViewColumns,
                connection, pkSlotIndexes, useServerTimestamp, onDupKeyBytes, maxSize, maxSizeBytes);
    }

    private static boolean isRowTimestampSet(int[] pkSlotIndexes, PTable table) {
        checkArgument(table.getRowTimestampColPos() != -1, "Call this method only for tables with row timestamp column");
        int rowTimestampColPKSlot = table.getRowTimestampColPos();
        for (int pkSlot : pkSlotIndexes) {
            if (pkSlot == rowTimestampColPKSlot) {
                return true;
            }
        }
        return false;
    }
    
    private static class UpdateColumnCompiler extends ExpressionCompiler {
        private PColumn column;
        
        private UpdateColumnCompiler(StatementContext context) {
            super(context);
        }

        public void setColumn(PColumn column) {
            this.column = column;
        }
        
        @Override
        public Expression visit(BindParseNode node) throws SQLException {
            if (isTopLevel()) {
                context.getBindManager().addParamMetaData(node, column);
                Object value = context.getBindManager().getBindValue(node);
                return LiteralExpression.newConstant(value, column.getDataType(), column.getSortOrder(), Determinism.ALWAYS);
            }
            return super.visit(node);
        }    
        
        @Override
        public Expression visit(LiteralParseNode node) throws SQLException {
            if (isTopLevel()) {
                return LiteralExpression.newConstant(node.getValue(), column.getDataType(), column.getSortOrder(), Determinism.ALWAYS);
            }
            return super.visit(node);
        }
    }
    
    private static class UpsertValuesCompiler extends UpdateColumnCompiler {
        private UpsertValuesCompiler(StatementContext context) {
            super(context);
        }
        
        @Override
        public Expression visit(SequenceValueParseNode node) throws SQLException {
            return context.getSequenceManager().newSequenceReference(node);
        }
    }
    

    private static SelectStatement prependTenantAndViewConstants(PTable table, SelectStatement select, String tenantId, Set<PColumn> addViewColumns, boolean useServerTimestamp) {
        if ((!table.isMultiTenant() || tenantId == null) && table.getViewIndexId() == null && addViewColumns.isEmpty() && !useServerTimestamp) {
            return select;
        }
        List<AliasedNode> selectNodes = newArrayListWithCapacity(select.getSelect().size() + 1 + addViewColumns.size());
        if (table.getViewIndexId() != null) {
            selectNodes.add(new AliasedNode(null, new LiteralParseNode(table.getViewIndexId())));
        }
        if (table.isMultiTenant() && tenantId != null) {
            selectNodes.add(new AliasedNode(null, new LiteralParseNode(tenantId)));
        }
        selectNodes.addAll(select.getSelect());
        for (PColumn column : addViewColumns) {
            byte[] byteValue = column.getViewConstant();
            Object value = column.getDataType().toObject(byteValue, 0, byteValue.length-1);
            selectNodes.add(new AliasedNode(null, new LiteralParseNode(value)));
        }
        if (useServerTimestamp) {
            PColumn rowTimestampCol = table.getPKColumns().get(table.getRowTimestampColPos());
            selectNodes.add(new AliasedNode(null, getNodeForRowTimestampColumn(rowTimestampCol)));
        }
        return SelectStatement.create(select, selectNodes);
    }
    
    /**
     * Check that none of no columns in our updatable VIEW are changing values.
     * @param tableRef
     * @param overlapViewColumns
     * @param targetColumns
     * @param projector
     * @throws SQLException
     */
    private static void throwIfNotUpdatable(TableRef tableRef, Set<PColumn> overlapViewColumns,
            List<PColumn> targetColumns, RowProjector projector, boolean sameTable) throws SQLException {
        PTable table = tableRef.getTable();
        if (table.getViewType() == ViewType.UPDATABLE && !overlapViewColumns.isEmpty()) {
            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            for (int i = 0; i < targetColumns.size(); i++) {
                PColumn targetColumn = targetColumns.get(i);
                if (overlapViewColumns.contains(targetColumn)) {
                    Expression source = projector.getColumnProjector(i).getExpression();
                    if (source.isStateless()) {
                        source.evaluate(null, ptr);
                        if (Bytes.compareTo(ptr.get(), ptr.getOffset(), ptr.getLength(), targetColumn.getViewConstant(), 0, targetColumn.getViewConstant().length-1) == 0) {
                            continue;
                        }
                    }
                    throw new SQLExceptionInfo.Builder(
                            SQLExceptionCode.CANNOT_UPDATE_VIEW_COLUMN)
                            .setColumnName(targetColumn.getName().getString())
                            .build().buildException();
                }
            }
        }
    }

    public class ServerUpsertSelectMutationPlan implements MutationPlan {
        private final QueryPlan queryPlan;
        private final TableRef tableRef;
        private final QueryPlan originalQueryPlan;
        private final StatementContext context;
        private final PhoenixConnection connection;
        private final Scan scan;
        private final QueryPlan aggPlan;
        private final RowProjector aggProjector;
        private final int maxSize;
        private final long maxSizeBytes;

        public ServerUpsertSelectMutationPlan(QueryPlan queryPlan, TableRef tableRef, QueryPlan originalQueryPlan,
                                              StatementContext context, PhoenixConnection connection,
                                              Scan scan, QueryPlan aggPlan, RowProjector aggProjector,
                                              int maxSize, long maxSizeBytes) {
            this.queryPlan = queryPlan;
            this.tableRef = tableRef;
            this.originalQueryPlan = originalQueryPlan;
            this.context = context;
            this.connection = connection;
            this.scan = scan;
            this.aggPlan = aggPlan;
            this.aggProjector = aggProjector;
            this.maxSize = maxSize;
            this.maxSizeBytes = maxSizeBytes;
        }

        @Override
        public ParameterMetaData getParameterMetaData() {
            return queryPlan.getContext().getBindManager().getParameterMetaData();
        }

        @Override
        public StatementContext getContext() {
            return queryPlan.getContext();
        }

        @Override
        public TableRef getTargetRef() {
            return tableRef;
        }

        @Override
        public QueryPlan getQueryPlan() {
            return aggPlan;
        }

        @Override
        public Set<TableRef> getSourceRefs() {
            return originalQueryPlan.getSourceRefs();
        }

        @Override
        public Operation getOperation() {
          return operation;
        }

        @Override
        public MutationState execute() throws SQLException {
            ImmutableBytesWritable ptr = context.getTempPtr();
            PTable table = tableRef.getTable();
            table.getIndexMaintainers(ptr, context.getConnection());
            ScanUtil.annotateScanWithMetadataAttributes(table, scan);
            byte[] txState = table.isTransactional() ?
                    connection.getMutationState().encodeTransaction() : ByteUtil.EMPTY_BYTE_ARRAY;

            ScanUtil.setClientVersion(scan, MetaDataProtocol.PHOENIX_VERSION);
            if (aggPlan.getTableRef().getTable().isTransactional() 
                    || (table.getType() == PTableType.INDEX && table.isTransactional())) {
                scan.setAttribute(BaseScannerRegionObserverConstants.TX_STATE, txState);
            }
            if (ptr.getLength() > 0) {
                byte[] uuidValue = ServerCacheClient.generateId();
                scan.setAttribute(PhoenixIndexCodec.INDEX_UUID, uuidValue);
                scan.setAttribute(PhoenixIndexCodec.INDEX_PROTO_MD, ptr.get());
            }
            ResultIterator iterator = aggPlan.iterator();
            try {
                Tuple row = iterator.next();
                final long mutationCount = (Long) aggProjector.getColumnProjector(0).getValue(row,
                        PLong.INSTANCE, ptr);
                return new MutationState(maxSize, maxSizeBytes, connection) {
                    @Override
                    public long getUpdateCount() {
                        return mutationCount;
                    }
                };
            } finally {
                iterator.close();
            }

        }

        @Override
        public ExplainPlan getExplainPlan() throws SQLException {
            ExplainPlan explainPlan = aggPlan.getExplainPlan();
            List<String> queryPlanSteps = explainPlan.getPlanSteps();
            ExplainPlanAttributes explainPlanAttributes =
                explainPlan.getPlanStepsAsAttributes();
            List<String> planSteps =
                Lists.newArrayListWithExpectedSize(queryPlanSteps.size() + 1);
            ExplainPlanAttributesBuilder newBuilder =
                new ExplainPlanAttributesBuilder(explainPlanAttributes);
            newBuilder.setAbstractExplainPlan("UPSERT ROWS");
            planSteps.add("UPSERT ROWS");
            planSteps.addAll(queryPlanSteps);
            return new ExplainPlan(planSteps, newBuilder.build());
        }

        @Override
        public Long getEstimatedRowsToScan() throws SQLException {
            return aggPlan.getEstimatedRowsToScan();
        }

        @Override
        public Long getEstimatedBytesToScan() throws SQLException {
            return aggPlan.getEstimatedBytesToScan();
        }

        @Override
        public Long getEstimateInfoTimestamp() throws SQLException {
            return aggPlan.getEstimateInfoTimestamp();
        }
    }

    private class UpsertValuesMutationPlan implements MutationPlan {
        private final StatementContext context;
        private final TableRef tableRef;
        private final int nodeIndexOffset;
        private final List<Expression> constantExpressions;
        private final List<PColumn> allColumns;
        private final int[] columnIndexes;
        private final Set<PColumn> overlapViewColumns;
        private final byte[][] values;
        private final Set<PColumn> addViewColumns;
        private final PhoenixConnection connection;
        private final int[] pkSlotIndexes;
        private final boolean useServerTimestamp;
        private final byte[] onDupKeyBytes;
        private final int maxSize;
        private final long maxSizeBytes;

        public UpsertValuesMutationPlan(StatementContext context, TableRef tableRef, int nodeIndexOffset,
                                        List<Expression> constantExpressions, List<PColumn> allColumns,
                                        int[] columnIndexes, Set<PColumn> overlapViewColumns, byte[][] values,
                                        Set<PColumn> addViewColumns, PhoenixConnection connection,
                                        int[] pkSlotIndexes, boolean useServerTimestamp, byte[] onDupKeyBytes,
                                        int maxSize, long maxSizeBytes) {
            this.context = context;
            this.tableRef = tableRef;
            this.nodeIndexOffset = nodeIndexOffset;
            this.constantExpressions = constantExpressions;
            this.allColumns = allColumns;
            this.columnIndexes = columnIndexes;
            this.overlapViewColumns = overlapViewColumns;
            this.values = values;
            this.addViewColumns = addViewColumns;
            this.connection = connection;
            this.pkSlotIndexes = pkSlotIndexes;
            this.useServerTimestamp = useServerTimestamp;
            this.onDupKeyBytes = onDupKeyBytes;
            this.maxSize = maxSize;
            this.maxSizeBytes = maxSizeBytes;
        }

        @Override
        public ParameterMetaData getParameterMetaData() {
            return context.getBindManager().getParameterMetaData();
        }

        @Override
        public StatementContext getContext() {
            return context;
        }

        @Override
        public TableRef getTargetRef() {
            return tableRef;
        }

        @Override
        public QueryPlan getQueryPlan() {
            return null;
        }

        @Override
        public Set<TableRef> getSourceRefs() {
            return Collections.emptySet();
        }

        @Override
        public Operation getOperation() {
          return operation;
        }

        @Override
        public MutationState execute() throws SQLException {
            ImmutableBytesWritable ptr = context.getTempPtr();
            final SequenceManager sequenceManager = context.getSequenceManager();
            // Next evaluate all the expressions
            int nodeIndex = nodeIndexOffset;
            PTable table = tableRef.getTable();
            Tuple tuple = sequenceManager.getSequenceCount() == 0 ? null :
                sequenceManager.newSequenceTuple(null);
            for (Expression constantExpression : constantExpressions) {
                PColumn column = allColumns.get(columnIndexes[nodeIndex]);
                constantExpression.evaluate(tuple, ptr);
                Object value = null;
                if (constantExpression.getDataType() != null) {
                    value = constantExpression.getDataType().toObject(ptr, constantExpression.getSortOrder(),
                            constantExpression.getMaxLength(), constantExpression.getScale());
                    if (!constantExpression.getDataType().isCoercibleTo(column.getDataType(), value)) {
                        throw TypeMismatchException.newException(
                            constantExpression.getDataType(), column.getDataType(), "expression: "
                                    + constantExpression.toString() + " in column " + column);
                    }
                    if (!column.getDataType().isSizeCompatible(ptr, value, constantExpression.getDataType(),
                            constantExpression.getSortOrder(), constantExpression.getMaxLength(),
                            constantExpression.getScale(), column.getMaxLength(), column.getScale())) {
                        throw new DataExceedsCapacityException(column.getDataType(), column.getMaxLength(),
                                column.getScale(), column.getName().getString());
                    }
                }
                column.getDataType().coerceBytes(ptr, value, constantExpression.getDataType(),
                        constantExpression.getMaxLength(), constantExpression.getScale(), constantExpression.getSortOrder(),
                        column.getMaxLength(), column.getScale(),column.getSortOrder(),
                        table.rowKeyOrderOptimizable());
                if (overlapViewColumns.contains(column) && Bytes.compareTo(ptr.get(), ptr.getOffset(), ptr.getLength(), column.getViewConstant(), 0, column.getViewConstant().length-1) != 0) {
                    throw new SQLExceptionInfo.Builder(
                            SQLExceptionCode.CANNOT_UPDATE_VIEW_COLUMN)
                            .setColumnName(column.getName().getString())
                            .setMessage("value=" + constantExpression.toString()).build().buildException();
                }
                values[nodeIndex] = ByteUtil.copyKeyBytesIfNecessary(ptr);
                nodeIndex++;
            }
            // Add columns based on view
            for (PColumn column : addViewColumns) {
                if (IndexUtil.getViewConstantValue(column, ptr)) {
                    values[nodeIndex++] = ByteUtil.copyKeyBytesIfNecessary(ptr);
                } else {
                    throw new IllegalStateException();
                }
            }
            MultiRowMutationState mutation = new MultiRowMutationState(1);
            IndexMaintainer indexMaintainer = null;
            byte[][] viewConstants = null;
            if (table.getIndexType() == IndexType.LOCAL) {
                PTable parentTable =
                        statement
                                .getConnection()
                                .getMetaDataCache()
                                .getTableRef(
                                    new PTableKey(statement.getConnection().getTenantId(),
                                            table.getParentName().getString())).getTable();
                indexMaintainer = table.getIndexMaintainer(parentTable, connection);
                viewConstants = IndexUtil.getViewConstants(parentTable);
            }
            int maxHBaseClientKeyValueSize = statement.getConnection().getQueryServices().getProps().
                    getInt(QueryServices.HBASE_CLIENT_KEYVALUE_MAXSIZE,
                            QueryServicesOptions.DEFAULT_HBASE_CLIENT_KEYVALUE_MAXSIZE);
            setValues(values, pkSlotIndexes, columnIndexes, table, mutation, statement, useServerTimestamp,
                    indexMaintainer, viewConstants, onDupKeyBytes, 0, maxHBaseClientKeyValueSize);
            return new MutationState(tableRef, mutation, 0, maxSize, maxSizeBytes, connection);
        }

        @Override
        public ExplainPlan getExplainPlan() throws SQLException {
            List<String> planSteps = Lists.newArrayListWithExpectedSize(2);
            if (context.getSequenceManager().getSequenceCount() > 0) {
                planSteps.add("CLIENT RESERVE " + context.getSequenceManager().getSequenceCount() + " SEQUENCES");
            }
            planSteps.add("PUT SINGLE ROW");
            return new ExplainPlan(planSteps);
        }

        @Override
        public Long getEstimatedRowsToScan() throws SQLException {
            return 0l;
        }

        @Override
        public Long getEstimatedBytesToScan() throws SQLException {
            return 0l;
        }

        @Override
        public Long getEstimateInfoTimestamp() throws SQLException {
            return 0l;
        }
    }

    private class ClientUpsertSelectMutationPlan implements MutationPlan {
        private final QueryPlan queryPlan;
        private final TableRef tableRef;
        private final QueryPlan originalQueryPlan;
        private final UpsertingParallelIteratorFactory parallelIteratorFactory;
        private final RowProjector projector;
        private final int[] columnIndexes;
        private final int[] pkSlotIndexes;
        private final boolean useServerTimestamp;
        private final int maxSize;
        private final long maxSizeBytes;

        public ClientUpsertSelectMutationPlan(QueryPlan queryPlan, TableRef tableRef, QueryPlan originalQueryPlan, UpsertingParallelIteratorFactory parallelIteratorFactory, RowProjector projector, int[] columnIndexes, int[] pkSlotIndexes, boolean useServerTimestamp, int maxSize, long maxSizeBytes) {
            this.queryPlan = queryPlan;
            this.tableRef = tableRef;
            this.originalQueryPlan = originalQueryPlan;
            this.parallelIteratorFactory = parallelIteratorFactory;
            this.projector = projector;
            this.columnIndexes = columnIndexes;
            this.pkSlotIndexes = pkSlotIndexes;
            this.useServerTimestamp = useServerTimestamp;
            this.maxSize = maxSize;
            this.maxSizeBytes = maxSizeBytes;
            queryPlan.getContext().setClientSideUpsertSelect(true);
        }

        @Override
        public ParameterMetaData getParameterMetaData() {
            return queryPlan.getContext().getBindManager().getParameterMetaData();
        }

        @Override
        public StatementContext getContext() {
            return queryPlan.getContext();
        }

        @Override
        public TableRef getTargetRef() {
            return tableRef;
        }

        @Override
        public QueryPlan getQueryPlan() {
            return queryPlan;
        }

        @Override
        public Set<TableRef> getSourceRefs() {
            return originalQueryPlan.getSourceRefs();
        }

        @Override
        public Operation getOperation() {
          return operation;
        }

        @Override
        public MutationState execute() throws SQLException {
            ResultIterator iterator = queryPlan.iterator();
            if (parallelIteratorFactory == null) {
                return upsertSelect(new StatementContext(statement, queryPlan.getContext().getScan()), tableRef, projector, iterator, columnIndexes, pkSlotIndexes, useServerTimestamp, false);
            }
            try {
                parallelIteratorFactory.setRowProjector(projector);
                parallelIteratorFactory.setColumnIndexes(columnIndexes);
                parallelIteratorFactory.setPkSlotIndexes(pkSlotIndexes);
                Tuple tuple;
                long totalRowCount = 0;
                StatementContext context = queryPlan.getContext();
                while ((tuple=iterator.next()) != null) {// Runs query
                    Cell kv = tuple.getValue(0);
                    totalRowCount += PLong.INSTANCE.getCodec().decodeLong(kv.getValueArray(), kv.getValueOffset(), SortOrder.getDefault());
                }
                // Return total number of rows that have been updated. In the case of auto commit being off
                // the mutations will all be in the mutation state of the current connection.
                MutationState mutationState = new MutationState(maxSize, maxSizeBytes, statement.getConnection(), totalRowCount);
                /*
                 *  All the metrics collected for measuring the reads done by the parallel mutating iterators
                 *  is included in the ReadMetricHolder of the statement context. Include these metrics in the
                 *  returned mutation state so they can be published on commit.
                 */
                mutationState.setReadMetricQueue(context.getReadMetricsQueue());
                return mutationState;
            } finally {
                iterator.close();
            }
        }

        @Override
        public ExplainPlan getExplainPlan() throws SQLException {
            ExplainPlan explainPlan = queryPlan.getExplainPlan();
            List<String> queryPlanSteps = explainPlan.getPlanSteps();
            ExplainPlanAttributes explainPlanAttributes =
                explainPlan.getPlanStepsAsAttributes();
            List<String> planSteps =
                Lists.newArrayListWithExpectedSize(queryPlanSteps.size() + 1);
            ExplainPlanAttributesBuilder newBuilder =
                new ExplainPlanAttributesBuilder(explainPlanAttributes);
            newBuilder.setAbstractExplainPlan("UPSERT SELECT");
            planSteps.add("UPSERT SELECT");
            planSteps.addAll(queryPlanSteps);
            return new ExplainPlan(planSteps, newBuilder.build());
        }

        @Override
        public Long getEstimatedRowsToScan() throws SQLException {
            return queryPlan.getEstimatedRowsToScan();
        }

        @Override
        public Long getEstimatedBytesToScan() throws SQLException {
            return queryPlan.getEstimatedBytesToScan();
        }

        @Override
        public Long getEstimateInfoTimestamp() throws SQLException {
            return queryPlan.getEstimateInfoTimestamp();
        }
    }
}