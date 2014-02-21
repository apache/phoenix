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

import static com.google.common.collect.Lists.newArrayListWithCapacity;

import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.cache.ServerCacheClient.ServerCache;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.coprocessor.UngroupedAggregateRegionObserver;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.AggregatePlan;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.IndexMetaDataCacheClient;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.iterate.ParallelIterators.ParallelIteratorFactory;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.SequenceResultIterator;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.optimize.QueryOptimizer;
import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.parse.BindParseNode;
import org.apache.phoenix.parse.ColumnName;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.ComparisonParseNode;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.parse.IsNullParseNode;
import org.apache.phoenix.parse.LiteralParseNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.SequenceValueParseNode;
import org.apache.phoenix.parse.UpsertStatement;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.ConstraintViolationException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.ViewType;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.ReadOnlyTableException;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class UpsertCompiler {
    private static void setValues(byte[][] values, int[] pkSlotIndex, int[] columnIndexes, PTable table, Map<ImmutableBytesPtr,Map<PColumn,byte[]>> mutation) {
        Map<PColumn,byte[]> columnValues = Maps.newHashMapWithExpectedSize(columnIndexes.length);
        byte[][] pkValues = new byte[table.getPKColumns().size()][];
        // If the table uses salting, the first byte is the salting byte, set to an empty array
        // here and we will fill in the byte later in PRowImpl.
        if (table.getBucketNum() != null) {
            pkValues[0] = new byte[] {0};
        }
        for (int i = 0; i < values.length; i++) {
            byte[] value = values[i];
            PColumn column = table.getColumns().get(columnIndexes[i]);
            if (SchemaUtil.isPKColumn(column)) {
                pkValues[pkSlotIndex[i]] = value;
            } else {
                columnValues.put(column, value);
            }
        }
        ImmutableBytesPtr ptr = new ImmutableBytesPtr();
        table.newKey(ptr, pkValues);
        mutation.put(ptr, columnValues);
    }

    private static MutationState upsertSelect(PhoenixStatement statement, 
            TableRef tableRef, RowProjector projector, ResultIterator iterator, int[] columnIndexes,
            int[] pkSlotIndexes) throws SQLException {
        try {
            PhoenixConnection connection = statement.getConnection();
            ConnectionQueryServices services = connection.getQueryServices();
            int maxSize = services.getProps().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB,QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
            int batchSize = Math.min(connection.getMutateBatchSize(), maxSize);
            boolean isAutoCommit = connection.getAutoCommit();
            byte[][] values = new byte[columnIndexes.length][];
            int rowCount = 0;
            Map<ImmutableBytesPtr,Map<PColumn,byte[]>> mutation = Maps.newHashMapWithExpectedSize(batchSize);
            PTable table = tableRef.getTable();
            ResultSet rs = new PhoenixResultSet(iterator, projector, statement);
            while (rs.next()) {
                for (int i = 0; i < values.length; i++) {
                    PColumn column = table.getColumns().get(columnIndexes[i]);
                    byte[] byteValue = rs.getBytes(i+1);
                    Object value = rs.getObject(i+1);
                    int rsPrecision = rs.getMetaData().getPrecision(i+1);
                    Integer precision = rsPrecision == 0 ? null : rsPrecision;
                    int rsScale = rs.getMetaData().getScale(i+1);
                    Integer scale = rsScale == 0 ? null : rsScale;
                    if (column.getSortOrder() == SortOrder.DESC) {
                        byte[] tempByteValue = Arrays.copyOf(byteValue, byteValue.length);
                        byteValue = SortOrder.invert(byteValue, 0, tempByteValue, 0, byteValue.length);
                    }
                    // We are guaranteed that the two column will have compatible types,
                    // as we checked that before.
                    if (!column.getDataType().isSizeCompatible(column.getDataType(),
                            value, byteValue,
                            precision, column.getMaxLength(), 
                            scale, column.getScale())) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.DATA_INCOMPATIBLE_WITH_TYPE)
                            .setColumnName(column.getName().getString()).build().buildException();
                    }
                    values[i] = column.getDataType().coerceBytes(byteValue, value, column.getDataType(),
                            precision, scale, column.getMaxLength(), column.getScale());
                }
                setValues(values, pkSlotIndexes, columnIndexes, table, mutation);
                rowCount++;
                // Commit a batch if auto commit is true and we're at our batch size
                if (isAutoCommit && rowCount % batchSize == 0) {
                    MutationState state = new MutationState(tableRef, mutation, 0, maxSize, connection);
                    connection.getMutationState().join(state);
                    connection.commit();
                    mutation.clear();
                }
            }
            // If auto commit is true, this last batch will be committed upon return
            return new MutationState(tableRef, mutation, rowCount / batchSize * batchSize, maxSize, connection);
        } finally {
            iterator.close();
        }
    }

    private static class UpsertingParallelIteratorFactory extends MutatingParallelIteratorFactory {
        private RowProjector projector;
        private int[] columnIndexes;
        private int[] pkSlotIndexes;

        private UpsertingParallelIteratorFactory (PhoenixConnection connection, TableRef tableRef) {
            super(connection, tableRef);
        }

        @Override
        protected MutationState mutate(StatementContext context, ResultIterator iterator, PhoenixConnection connection) throws SQLException {
            PhoenixStatement statement = new PhoenixStatement(connection);
            // Clone the connection as it's not thread safe and will be operated on in parallel
            if (context.getSequenceManager().getSequenceCount() > 0) {
                iterator = new SequenceResultIterator(iterator,context.getSequenceManager());
            }
            return upsertSelect(statement, tableRef, projector, iterator, columnIndexes, pkSlotIndexes);
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
    
    public UpsertCompiler(PhoenixStatement statement) {
        this.statement = statement;
    }
    
    public MutationPlan compile(UpsertStatement upsert) throws SQLException {
        final PhoenixConnection connection = statement.getConnection();
        ConnectionQueryServices services = connection.getQueryServices();
        final int maxSize = services.getProps().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB,QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
        final ColumnResolver resolver = FromCompiler.getResolver(upsert, connection);
        final TableRef tableRef = resolver.getTables().get(0);
        PTable table = tableRef.getTable();
        if (table.getType() == PTableType.VIEW) {
            if (table.getViewType().isReadOnly()) {
                throw new ReadOnlyTableException(table.getSchemaName().getString(),table.getTableName().getString());
            }
        }
        boolean isSalted = table.getBucketNum() != null;
        boolean isTenantSpecific = table.isMultiTenant() && connection.getTenantId() != null;
        boolean isSharedViewIndex = table.getViewIndexId() != null;
        String tenantId = isTenantSpecific ? connection.getTenantId().getString() : null;
        int posOffset = isSalted ? 1 : 0;
        // Setup array of column indexes parallel to values that are going to be set
        List<ColumnName> columnNodes = upsert.getColumns();
        List<PColumn> allColumns = table.getColumns();
        Map<ColumnRef, byte[]> addViewColumns = Collections.emptyMap();
        Map<PColumn, byte[]> overlapViewColumns = Collections.emptyMap();

        int[] columnIndexesToBe;
        int nColumnsToSet = 0;
        int[] pkSlotIndexesToBe;
        List<PColumn> targetColumns;
        if (table.getViewType() == ViewType.UPDATABLE) {
            StatementContext context = new StatementContext(statement, resolver, this.statement.getParameters(), new Scan());
            ViewValuesMapBuilder builder = new ViewValuesMapBuilder(context);
            ParseNode viewNode = new SQLParser(table.getViewStatement()).parseQuery().getWhere();
            viewNode.accept(builder);
            addViewColumns = builder.getViewColumns();
        }
        // Allow full row upsert if no columns or only dynamic ones are specified and values count match
        if (columnNodes.isEmpty() || columnNodes.size() == upsert.getTable().getDynamicColumns().size()) {
            nColumnsToSet = allColumns.size() - posOffset;
            columnIndexesToBe = new int[nColumnsToSet];
            pkSlotIndexesToBe = new int[columnIndexesToBe.length];
            targetColumns = Lists.newArrayListWithExpectedSize(columnIndexesToBe.length);
            targetColumns.addAll(Collections.<PColumn>nCopies(columnIndexesToBe.length, null));
            for (int i = posOffset, j = posOffset; i < allColumns.size(); i++) {
                PColumn column = allColumns.get(i);
                columnIndexesToBe[i-posOffset] = i;
                targetColumns.set(i-posOffset, column);
                if (SchemaUtil.isPKColumn(column)) {
                    pkSlotIndexesToBe[i-posOffset] = j++;
                }
            }
            if (!addViewColumns.isEmpty()) {
                // All view columns overlap in this case
                overlapViewColumns = Maps.newHashMapWithExpectedSize(addViewColumns.size());
                for (Map.Entry<ColumnRef, byte[]> entry : addViewColumns.entrySet()) {
                    ColumnRef ref = entry.getKey();
                    PColumn column = ref.getColumn();
                    overlapViewColumns.put(column, entry.getValue());
                }
                addViewColumns.clear();
            }
        } else {
            // Size for worse case
            int numColsInUpsert = columnNodes.size();
            nColumnsToSet = numColsInUpsert + addViewColumns.size() + (isTenantSpecific ? 1 : 0) +  + (isSharedViewIndex ? 1 : 0);
            columnIndexesToBe = new int[nColumnsToSet];
            pkSlotIndexesToBe = new int[columnIndexesToBe.length];
            targetColumns = Lists.newArrayListWithExpectedSize(columnIndexesToBe.length);
            targetColumns.addAll(Collections.<PColumn>nCopies(columnIndexesToBe.length, null));
            Arrays.fill(columnIndexesToBe, -1); // TODO: necessary? So we'll get an AIOB exception if it's not replaced
            Arrays.fill(pkSlotIndexesToBe, -1); // TODO: necessary? So we'll get an AIOB exception if it's not replaced
            BitSet pkColumnsSet = new BitSet(table.getPKColumns().size());
            int i = 0;
            for (i = 0; i < numColsInUpsert; i++) {
                ColumnName colName = columnNodes.get(i);
                ColumnRef ref = resolver.resolveColumn(null, colName.getFamilyName(), colName.getColumnName());
                PColumn column = ref.getColumn();
                byte[] viewValue = addViewColumns.remove(ref);
                if (viewValue != null) {
                    if (overlapViewColumns.isEmpty()) {
                        overlapViewColumns = Maps.newHashMapWithExpectedSize(addViewColumns.size());
                    }
                    nColumnsToSet--;
                    overlapViewColumns.put(column, viewValue);
                }
                columnIndexesToBe[i] = ref.getColumnPosition();
                targetColumns.set(i, column);
                if (SchemaUtil.isPKColumn(column)) {
                    pkColumnsSet.set(pkSlotIndexesToBe[i] = ref.getPKSlotPosition());
                }
            }
            for (Map.Entry<ColumnRef, byte[]> entry : addViewColumns.entrySet()) {
                ColumnRef ref = entry.getKey();
                PColumn column = ref.getColumn();
                columnIndexesToBe[i] = ref.getColumnPosition();
                targetColumns.set(i, column);
                if (SchemaUtil.isPKColumn(column)) {
                    pkColumnsSet.set(pkSlotIndexesToBe[i] = ref.getPKSlotPosition());
                }
                i++;
            }
            // Add tenant column directly, as we don't want to resolve it as this will fail
            if (isTenantSpecific) {
                PColumn tenantColumn = table.getPKColumns().get(posOffset);
                columnIndexesToBe[i] = tenantColumn.getPosition();
                pkColumnsSet.set(pkSlotIndexesToBe[i] = posOffset);
                targetColumns.set(i, tenantColumn);
                i++;
            }
            if (isSharedViewIndex) {
                PColumn indexIdColumn = table.getPKColumns().get(posOffset+1);
                columnIndexesToBe[i] = indexIdColumn.getPosition();
                pkColumnsSet.set(pkSlotIndexesToBe[i] = posOffset+1);
                targetColumns.set(i, indexIdColumn);
                i++;
            }
            i = posOffset;
            for ( ; i < table.getPKColumns().size(); i++) {
                PColumn pkCol = table.getPKColumns().get(i);
                if (!pkColumnsSet.get(i)) {
                    if (!pkCol.isNullable()) {
                        throw new ConstraintViolationException(table.getName().getString() + "." + pkCol.getName().getString() + " may not be null");
                    }
                }
            }
        }
        
        List<ParseNode> valueNodes = upsert.getValues();
        QueryPlan plan = null;
        RowProjector rowProjectorToBe = null;
        int nValuesToSet;
        boolean sameTable = false;
        boolean runOnServer = false;
        UpsertingParallelIteratorFactory upsertParallelIteratorFactoryToBe = null;
        final boolean isAutoCommit = connection.getAutoCommit();
        if (valueNodes == null) {
            SelectStatement select = upsert.getSelect();
            assert(select != null);
            ColumnResolver selectResolver = FromCompiler.getResolver(select, connection);
            select = StatementNormalizer.normalize(select, selectResolver);
            select = addTenantAndViewConstants(table, select, tenantId, addViewColumns);
            sameTable = select.getFrom().size() == 1
                && tableRef.equals(selectResolver.getTables().get(0));
            /* We can run the upsert in a coprocessor if:
             * 1) from has only 1 table and the into table matches from table
             * 2) the select query isn't doing aggregation
             * 3) autoCommit is on
             * 4) the table is not immutable, as the client is the one that figures out the additional
             *    puts for index tables.
             * 5) no limit clause
             * Otherwise, run the query to pull the data from the server
             * and populate the MutationState (upto a limit).
            */            
            runOnServer = sameTable && isAutoCommit && !table.isImmutableRows() && !select.isAggregate() && !select.isDistinct() && select.getLimit() == null && table.getBucketNum() == null;
            ParallelIteratorFactory parallelIteratorFactory;
            if (select.isAggregate() || select.isDistinct() || select.getLimit() != null) {
                parallelIteratorFactory = null;
            } else {
                // We can pipeline the upsert select instead of spooling everything to disk first,
                // if we don't have any post processing that's required.
                parallelIteratorFactory = upsertParallelIteratorFactoryToBe = new UpsertingParallelIteratorFactory(connection, tableRef);
            }
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
            plan = new QueryOptimizer(services).optimize(statement, select, selectResolver, targetColumns, parallelIteratorFactory);
            runOnServer &= plan.getTableRef().equals(tableRef);
            rowProjectorToBe = plan.getProjector();
            nValuesToSet = rowProjectorToBe.getColumnCount();
            // Cannot auto commit if doing aggregation or topN or salted
            // Salted causes problems because the row may end up living on a different region
        } else {
            nValuesToSet = valueNodes.size() + addViewColumns.size() + (isTenantSpecific ? 1 : 0) + (isSharedViewIndex ? 1 : 0);
        }
        final RowProjector projector = rowProjectorToBe;
        final UpsertingParallelIteratorFactory upsertParallelIteratorFactory = upsertParallelIteratorFactoryToBe;
        final QueryPlan queryPlan = plan;
        // Resize down to allow a subset of columns to be specifiable
        if (columnNodes.isEmpty() && columnIndexesToBe.length >= nValuesToSet) {
            nColumnsToSet = nValuesToSet;
            columnIndexesToBe = Arrays.copyOf(columnIndexesToBe, nValuesToSet);
            pkSlotIndexesToBe = Arrays.copyOf(pkSlotIndexesToBe, nValuesToSet);
        }
        
        if (nValuesToSet != nColumnsToSet) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.UPSERT_COLUMN_NUMBERS_MISMATCH)
                .setMessage("Numbers of columns: " + nColumnsToSet + ". Number of values: " + nValuesToSet)
                .build().buildException();
        }
        
        final int[] columnIndexes = columnIndexesToBe;
        final int[] pkSlotIndexes = pkSlotIndexesToBe;
        
        // TODO: break this up into multiple functions
        ////////////////////////////////////////////////////////////////////
        // UPSERT SELECT
        /////////////////////////////////////////////////////////////////////
        if (valueNodes == null) {
            // Before we re-order, check that for updatable view columns
            // the projected expression either matches the column name or
            // is a constant with the same required value.
            throwIfNotUpdatable(tableRef, overlapViewColumns, targetColumns, projector, sameTable);
            
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
                        Expression literalNull = LiteralExpression.newConstant(null, column.getDataType(), true);
                        projectedExpressions.add(literalNull);
                        allColumnsIndexes[pos] = column.getPosition();
                    } 
                    // Swap select expression at pos with i
                    Collections.swap(projectedExpressions, i, pos);
                    // Swap column indexes and reverse column indexes too
                    int tempPos = allColumnsIndexes[i];
                    allColumnsIndexes[i] = allColumnsIndexes[pos];
                    allColumnsIndexes[pos] = tempPos;
                    reverseColumnIndexes[tempPos] = reverseColumnIndexes[i];
                    reverseColumnIndexes[i] = i;
                }
                // If any pk slots are changing, be conservative and don't run this server side.
                // If the row ends up living in a different region, we'll get an error otherwise.
                for (int i = 0; i < table.getPKColumns().size(); i++) {
                    PColumn column = table.getPKColumns().get(i);
                    Expression source = projectedExpressions.get(i);
                    if (source == null || !source.equals(new ColumnRef(tableRef, column.getPosition()).newColumnExpression())) {
                        // TODO: we could check the region boundaries to see if the pk will still be in it.
                        runOnServer = false; // bail on running server side, since PK may be changing
                        break;
                    }
                }
                
                ////////////////////////////////////////////////////////////////////
                // UPSERT SELECT run server-side
                /////////////////////////////////////////////////////////////////////
                if (runOnServer) {
                    // Iterate through columns being projected
                    List<PColumn> projectedColumns = Lists.newArrayListWithExpectedSize(projectedExpressions.size());
                    for (int i = 0; i < projectedExpressions.size(); i++) {
                        // Must make new column if position has changed
                        PColumn column = allColumns.get(allColumnsIndexes[i]);
                        projectedColumns.add(column.getPosition() == i ? column : new PColumnImpl(column, i));
                    }
                    // Build table from projectedColumns
                    PTable projectedTable = PTableImpl.makePTable(table, projectedColumns);
                    
                    SelectStatement select = SelectStatement.create(SelectStatement.COUNT_ONE, upsert.getHint());
                    final RowProjector aggProjector = ProjectionCompiler.compile(queryPlan.getContext(), select, GroupBy.EMPTY_GROUP_BY);
                    /*
                     * Transfer over PTable representing subset of columns selected, but all PK columns.
                     * Move columns setting PK first in pkSlot order, adding LiteralExpression of null for any missing ones.
                     * Transfer over List<Expression> for projection.
                     * In region scan, evaluate expressions in order, collecting first n columns for PK and collection non PK in mutation Map
                     * Create the PRow and get the mutations, adding them to the batch
                     */
                    final StatementContext context = queryPlan.getContext();
                    final Scan scan = context.getScan();
                    scan.setAttribute(UngroupedAggregateRegionObserver.UPSERT_SELECT_TABLE, UngroupedAggregateRegionObserver.serialize(projectedTable));
                    scan.setAttribute(UngroupedAggregateRegionObserver.UPSERT_SELECT_EXPRS, UngroupedAggregateRegionObserver.serialize(projectedExpressions));
                    // Ignore order by - it has no impact
                    final QueryPlan aggPlan = new AggregatePlan(context, select, tableRef, aggProjector, null, OrderBy.EMPTY_ORDER_BY, null, GroupBy.EMPTY_GROUP_BY, null);
                    return new MutationPlan() {
    
                        @Override
                        public PhoenixConnection getConnection() {
                            return connection;
                        }
    
                        @Override
                        public ParameterMetaData getParameterMetaData() {
                            return queryPlan.getContext().getBindManager().getParameterMetaData();
                        }
    
                        @Override
                        public MutationState execute() throws SQLException {
                            ImmutableBytesWritable ptr = context.getTempPtr();
                            tableRef.getTable().getIndexMaintainers(ptr);
                            ServerCache cache = null;
                            try {
                                if (ptr.getLength() > 0) {
                                    IndexMetaDataCacheClient client = new IndexMetaDataCacheClient(connection, tableRef);
                                    cache = client.addIndexMetadataCache(context.getScanRanges(), ptr);
                                    byte[] uuidValue = cache.getId();
                                    scan.setAttribute(PhoenixIndexCodec.INDEX_UUID, uuidValue);
                                }
                                ResultIterator iterator = aggPlan.iterator();
                                try {
                                    Tuple row = iterator.next();
                                    final long mutationCount = (Long)aggProjector.getColumnProjector(0).getValue(row, PDataType.LONG, ptr);
                                    return new MutationState(maxSize, connection) {
                                        @Override
                                        public long getUpdateCount() {
                                            return mutationCount;
                                        }
                                    };
                                } finally {
                                    iterator.close();
                                }
                            } finally {
                                if (cache != null) {
                                    cache.close();
                                }
                            }
                        }
    
                        @Override
                        public ExplainPlan getExplainPlan() throws SQLException {
                            List<String> queryPlanSteps =  aggPlan.getExplainPlan().getPlanSteps();
                            List<String> planSteps = Lists.newArrayListWithExpectedSize(queryPlanSteps.size()+1);
                            planSteps.add("UPSERT ROWS");
                            planSteps.addAll(queryPlanSteps);
                            return new ExplainPlan(planSteps);
                        }
                    };
                }
            }

            ////////////////////////////////////////////////////////////////////
            // UPSERT SELECT run client-side
            /////////////////////////////////////////////////////////////////////
            return new MutationPlan() {

                @Override
                public PhoenixConnection getConnection() {
                    return connection;
                }
                
                @Override
                public ParameterMetaData getParameterMetaData() {
                    return queryPlan.getContext().getBindManager().getParameterMetaData();
                }

                @Override
                public MutationState execute() throws SQLException {
                    ResultIterator iterator = queryPlan.iterator();
                    if (upsertParallelIteratorFactory == null) {
                        return upsertSelect(statement, tableRef, projector, iterator, columnIndexes, pkSlotIndexes);
                    }
                    upsertParallelIteratorFactory.setRowProjector(projector);
                    upsertParallelIteratorFactory.setColumnIndexes(columnIndexes);
                    upsertParallelIteratorFactory.setPkSlotIndexes(pkSlotIndexes);
                    Tuple tuple;
                    long totalRowCount = 0;
                    while ((tuple=iterator.next()) != null) {// Runs query
                        KeyValue kv = tuple.getValue(0);
                        totalRowCount += PDataType.LONG.getCodec().decodeLong(kv.getBuffer(), kv.getValueOffset(), SortOrder.getDefault());
                    }
                    // Return total number of rows that have been updated. In the case of auto commit being off
                    // the mutations will all be in the mutation state of the current connection.
                    return new MutationState(maxSize, statement.getConnection(), totalRowCount);
                }

                @Override
                public ExplainPlan getExplainPlan() throws SQLException {
                    List<String> queryPlanSteps =  queryPlan.getExplainPlan().getPlanSteps();
                    List<String> planSteps = Lists.newArrayListWithExpectedSize(queryPlanSteps.size()+1);
                    planSteps.add("UPSERT SELECT");
                    planSteps.addAll(queryPlanSteps);
                    return new ExplainPlan(planSteps);
                }
                
            };
        }

            
        ////////////////////////////////////////////////////////////////////
        // UPSERT VALUES
        /////////////////////////////////////////////////////////////////////
        int nodeIndex = 0;
        // Allocate array based on size of all columns in table,
        // since some values may not be set (if they're nullable).
        final StatementContext context = new StatementContext(statement, resolver, statement.getParameters(), new Scan());
        ImmutableBytesWritable ptr = context.getTempPtr();
        UpsertValuesCompiler expressionBuilder = new UpsertValuesCompiler(context);
        List<Expression> constantExpressions = Lists.newArrayListWithExpectedSize(valueNodes.size());
        // First build all the expressions, as with sequences we want to collect them all first
        // and initialize them in one batch
        for (ParseNode valueNode : valueNodes) {
            if (!valueNode.isStateless()) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.VALUE_IN_UPSERT_NOT_CONSTANT).build().buildException();
            }
            PColumn column = allColumns.get(columnIndexes[nodeIndex]);
            expressionBuilder.setColumn(column);
            constantExpressions.add(valueNode.accept(expressionBuilder));
            nodeIndex++;
        }
        final SequenceManager sequenceManager = context.getSequenceManager();
        sequenceManager.reserveSequences();
        // Next evaluate all the expressions
        nodeIndex = 0;
        final byte[][] values = new byte[nValuesToSet][];
        Tuple tuple = sequenceManager.getSequenceCount() == 0 ? null :
            sequenceManager.newSequenceTuple(null);
        for (Expression constantExpression : constantExpressions) {
            PColumn column = allColumns.get(columnIndexes[nodeIndex]);
            constantExpression.evaluate(tuple, ptr);
            Object value = null;
            byte[] byteValue = ByteUtil.copyKeyBytesIfNecessary(ptr);
            if (constantExpression.getDataType() != null) {
                // If SortOrder from expression in SELECT doesn't match the
                // column being projected into then invert the bits.
                if (constantExpression.getSortOrder() != column.getSortOrder()) {
                    byte[] tempByteValue = Arrays.copyOf(byteValue, byteValue.length);
                    byteValue = SortOrder.invert(byteValue, 0, tempByteValue, 0, byteValue.length);
                }
                value = constantExpression.getDataType().toObject(byteValue);
                if (!constantExpression.getDataType().isCoercibleTo(column.getDataType(), value)) { 
                    throw TypeMismatchException.newException(
                        constantExpression.getDataType(), column.getDataType(), "expression: "
                                + constantExpression.toString() + " in column " + column);
                }
                if (!column.getDataType().isSizeCompatible(constantExpression.getDataType(),
                        value, byteValue, constantExpression.getMaxLength(),
                        column.getMaxLength(), constantExpression.getScale(), column.getScale())) { 
                    throw new SQLExceptionInfo.Builder(
                        SQLExceptionCode.DATA_INCOMPATIBLE_WITH_TYPE).setColumnName(column.getName().getString())
                        .setMessage("value=" + constantExpression.toString()).build().buildException();
                }
            }
            byteValue = column.getDataType().coerceBytes(byteValue, value,
                    constantExpression.getDataType(), constantExpression.getMaxLength(), constantExpression.getScale(),
                    column.getMaxLength(), column.getScale());
            byte[] viewValue = overlapViewColumns.get(column);
            if (viewValue != null && Bytes.compareTo(byteValue, viewValue) != 0) {
                throw new SQLExceptionInfo.Builder(
                        SQLExceptionCode.CANNOT_UPDATE_VIEW_COLUMN)
                        .setColumnName(column.getName().getString())
                        .setMessage("value=" + constantExpression.toString()).build().buildException();
            }
            values[nodeIndex] = byteValue;
            nodeIndex++;
        }
        // Add columns based on view
        for (byte[] value : addViewColumns.values()) {
            values[nodeIndex++] = value;
        }
        if (isTenantSpecific) {
            values[nodeIndex++] = connection.getTenantId().getBytes();
        }
        if (isSharedViewIndex) {
            values[nodeIndex++] = MetaDataUtil.getViewIndexIdDataType().toBytes(table.getViewIndexId());
        }
        return new MutationPlan() {

            @Override
            public PhoenixConnection getConnection() {
                return connection;
            }

            @Override
            public ParameterMetaData getParameterMetaData() {
                return context.getBindManager().getParameterMetaData();
            }

            @Override
            public MutationState execute() {
                Map<ImmutableBytesPtr, Map<PColumn, byte[]>> mutation = Maps.newHashMapWithExpectedSize(1);
                setValues(values, pkSlotIndexes, columnIndexes, tableRef.getTable(), mutation);
                return new MutationState(tableRef, mutation, 0, maxSize, connection);
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

        };
    }
    
    private static final class UpsertValuesCompiler extends ExpressionCompiler {
        private PColumn column;
        
        private UpsertValuesCompiler(StatementContext context) {
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
                return LiteralExpression.newConstant(value, column.getDataType(), column.getSortOrder(), true);
            }
            return super.visit(node);
        }    
        
        @Override
        public Expression visit(LiteralParseNode node) throws SQLException {
            if (isTopLevel()) {
                return LiteralExpression.newConstant(node.getValue(), column.getDataType(), column.getSortOrder(), true);
            }
            return super.visit(node);
        }
        
        
        @Override
        public Expression visit(SequenceValueParseNode node) throws SQLException {
            return context.getSequenceManager().newSequenceReference(node);
        }
    }
    

    // ExpressionCompiler needs a context
    private static class ViewValuesMapBuilder extends ExpressionCompiler {
        private ColumnRef columnRef;
        private Map<ColumnRef, byte[]> viewColumns = Maps.newHashMapWithExpectedSize(5);

        private ViewValuesMapBuilder(StatementContext context) {
            super(context);
        }
        
        public Map<ColumnRef, byte[]> getViewColumns() {
            return viewColumns;
        }

        @Override
        protected ColumnRef resolveColumn(ColumnParseNode node) throws SQLException {
            return columnRef = super.resolveColumn(node);
        }

        @Override
        public Expression visitLeave(IsNullParseNode node, List<Expression> children) throws SQLException {
            viewColumns.put(columnRef, ByteUtil.EMPTY_BYTE_ARRAY);
            return super.visitLeave(node, children);
        }
        
        @Override
        public Expression visitLeave(ComparisonParseNode node, List<Expression> children) throws SQLException {
            Expression literal = children.get(1);
            ImmutableBytesWritable ptr = context.getTempPtr();
            literal.evaluate(null, ptr);
            PColumn column = columnRef.getColumn();
            column.getDataType().coerceBytes(ptr, literal.getDataType(), literal.getSortOrder(), column.getSortOrder());
            viewColumns.put(columnRef, ByteUtil.copyKeyBytesIfNecessary(ptr));
            return super.visitLeave(node, children);
        }
    }
    
    private static SelectStatement addTenantAndViewConstants(PTable table, SelectStatement select, String tenantId, Map<ColumnRef, byte[]> addViewColumns) {
        if (tenantId == null && addViewColumns.isEmpty()) {
            return select;
        }
        List<AliasedNode> selectNodes = newArrayListWithCapacity(select.getSelect().size() + 1 + addViewColumns.size());
        selectNodes.addAll(select.getSelect());
        for (Map.Entry<ColumnRef, byte[]> entry : addViewColumns.entrySet()) {
            ColumnRef ref = entry.getKey();
            PColumn column = ref.getColumn();
            byte[] byteValue = entry.getValue();
            Object value = column.getDataType().toObject(byteValue);
            selectNodes.add(new AliasedNode(null, new LiteralParseNode(value)));
        }
        if (table.isMultiTenant() && tenantId != null) {
            selectNodes.add(new AliasedNode(null, new LiteralParseNode(tenantId)));
        }
        if (table.getViewIndexId() != null) {
            selectNodes.add(new AliasedNode(null, new LiteralParseNode(table.getViewIndexId())));
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
    private static void throwIfNotUpdatable(TableRef tableRef, Map<PColumn, byte[]> overlapViewColumns,
            List<PColumn> targetColumns, RowProjector projector, boolean sameTable) throws SQLException {
        PTable table = tableRef.getTable();
        if (table.getViewType() == ViewType.UPDATABLE && !overlapViewColumns.isEmpty()) {
            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            for (int i = 0; i < targetColumns.size(); i++) {
                // Must make new column if position has changed
                PColumn targetColumn = targetColumns.get(i);
                byte[] value = overlapViewColumns.get(targetColumn);
                if (value != null) {
                    Expression source = projector.getColumnProjector(i).getExpression();
                    if (source == null) { // FIXME: is this possible?
                    } else if (source.isStateless()) {
                        source.evaluate(null, ptr);
                        if (Bytes.compareTo(ptr.get(), ptr.getOffset(), ptr.getLength(), value, 0, value.length) == 0) {
                            continue;
                        }
                    // TODO: we had a ColumnRef already in our map before
                    } else if (sameTable && source.equals(new ColumnRef(tableRef, targetColumn.getPosition()).newColumnExpression())) {
                        continue;
                    }
                    // TODO: one other check we could do is if the source is an updatable VIEW,
                    // check if the source column is a VIEW column with the same constant value
                    // as expected.
                    throw new SQLExceptionInfo.Builder(
                            SQLExceptionCode.CANNOT_UPDATE_VIEW_COLUMN)
                            .setColumnName(targetColumn.getName().getString())
                            .build().buildException();
                }
            }
        }
    }
}
