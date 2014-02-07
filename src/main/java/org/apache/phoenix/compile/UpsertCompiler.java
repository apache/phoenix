/*
 * Copyright 2014 The Apache Software Foundation
 *
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hbase.index.util.ImmutableBytesPtr;
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
import org.apache.phoenix.index.IndexMetaDataCacheClient;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.iterate.ParallelIterators.ParallelIteratorFactory;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.optimize.QueryOptimizer;
import org.apache.phoenix.parse.BindParseNode;
import org.apache.phoenix.parse.ColumnName;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.parse.LiteralParseNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.UpsertStatement;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.query.Scanner;
import org.apache.phoenix.schema.ColumnModifier;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.ConstraintViolationException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.ReadOnlyTableException;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.SchemaUtil;

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
                    // If ColumnModifier from expression in SELECT doesn't match the
                    // column being projected into then invert the bits.
                    if (column.getColumnModifier() == ColumnModifier.SORT_DESC) {
                        byte[] tempByteValue = Arrays.copyOf(byteValue, byteValue.length);
                        byteValue = ColumnModifier.SORT_DESC.apply(byteValue, 0, tempByteValue, 0, byteValue.length);
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
        protected MutationState mutate(PhoenixConnection connection, ResultIterator iterator) throws SQLException {
            PhoenixStatement statement = new PhoenixStatement(connection);
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
            throw new ReadOnlyTableException("Mutations not allowed for a view (" + tableRef + ")");
        }
        // Setup array of column indexes parallel to values that are going to be set
        List<ColumnName> columnNodes = upsert.getColumns();
        List<PColumn> allColumns = table.getColumns();

        int[] columnIndexesToBe;
        int[] pkSlotIndexesToBe;
        List<PColumn> targetColumns;
        boolean isSalted = table.getBucketNum() != null;
        int posOffset = isSalted ? 1 : 0;
        // Allow full row upsert if no columns or only dynamic one are specified and values count match
        int numColsInUpsert = columnNodes.size();
        int numColsInTable = table.getColumns().size();
        if (columnNodes.isEmpty() || numColsInUpsert == upsert.getTable().getDynamicColumns().size()) {
            columnIndexesToBe = new int[allColumns.size() - posOffset];
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
        } else {
            columnIndexesToBe = new int[numColsInUpsert];
            pkSlotIndexesToBe = new int[columnIndexesToBe.length];
            targetColumns = Lists.newArrayListWithExpectedSize(columnIndexesToBe.length);
            targetColumns.addAll(Collections.<PColumn>nCopies(columnIndexesToBe.length, null));
            Arrays.fill(columnIndexesToBe, -1); // TODO: necessary? So we'll get an AIOB exception if it's not replaced
            Arrays.fill(pkSlotIndexesToBe, -1); // TODO: necessary? So we'll get an AIOB exception if it's not replaced
            BitSet pkColumnsSet = new BitSet(table.getPKColumns().size());
            for (int i =0; i < numColsInUpsert; i++) {
                ColumnName colName = columnNodes.get(i);
                ColumnRef ref = resolver.resolveColumn(null, colName.getFamilyName(), colName.getColumnName());
                columnIndexesToBe[i] = ref.getColumnPosition();
                targetColumns.set(i, ref.getColumn());
                if (SchemaUtil.isPKColumn(ref.getColumn())) {
                    pkColumnsSet.set(pkSlotIndexesToBe[i] = ref.getPKSlotPosition());
                }
            }
            int i = posOffset;
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
        boolean runOnServer = false;
        UpsertingParallelIteratorFactory upsertParallelIteratorFactoryToBe = null;
        final boolean isAutoCommit = connection.getAutoCommit();
        if (valueNodes == null) {
            SelectStatement select = upsert.getSelect();
            assert(select != null);
            TableRef selectTableRef = FromCompiler.getResolver(select, connection).getTables().get(0);
            boolean sameTable = tableRef.equals(selectTableRef);
            /* We can run the upsert in a coprocessor if:
             * 1) the into table matches from table
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
            // TODO: once MutationState is thread safe, then when auto commit is off, we can still run in parallel
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
            plan = new QueryOptimizer(services).optimize(select, statement, targetColumns, parallelIteratorFactory);
            runOnServer &= plan.getTableRef().equals(tableRef);
            rowProjectorToBe = plan.getProjector();
            nValuesToSet = rowProjectorToBe.getColumnCount();
            // Cannot auto commit if doing aggregation or topN or salted
            // Salted causes problems because the row may end up living on a different region
        } else {
            nValuesToSet = valueNodes.size();
        }
        final RowProjector projector = rowProjectorToBe;
        final UpsertingParallelIteratorFactory upsertParallelIteratorFactory = upsertParallelIteratorFactoryToBe;
        final QueryPlan queryPlan = plan;
        // Resize down to allow a subset of columns to be specifiable
        if (columnNodes.isEmpty()) {
            columnIndexesToBe = Arrays.copyOf(columnIndexesToBe, nValuesToSet);
            pkSlotIndexesToBe = Arrays.copyOf(pkSlotIndexesToBe, nValuesToSet);
        }
        
        if (nValuesToSet != columnIndexesToBe.length) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.UPSERT_COLUMN_NUMBERS_MISMATCH)
                .setMessage("Numbers of columns: " + columnIndexesToBe.length + ". Number of values: " + nValuesToSet)
                .build().buildException();
        }
        
        final int[] columnIndexes = columnIndexesToBe;
        final int[] pkSlotIndexes = pkSlotIndexesToBe;
        
        // TODO: break this up into multiple functions
        ////////////////////////////////////////////////////////////////////
        // UPSERT SELECT
        /////////////////////////////////////////////////////////////////////
        if (valueNodes == null) {
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
                        Expression literalNull = LiteralExpression.newConstant(null, column.getDataType());
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
                                Scanner scanner = aggPlan.getScanner();
                                ResultIterator iterator = scanner.iterator();
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
                    Scanner scanner = queryPlan.getScanner();
                    ResultIterator iterator = scanner.iterator();
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
                        totalRowCount += PDataType.LONG.getCodec().decodeLong(kv.getBuffer(), kv.getValueOffset(), null);
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
        if (nValuesToSet > numColsInTable) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.UPSERT_COLUMN_NUMBERS_MISMATCH)
                .setMessage("Numbers of columns in table: " + numColsInTable + ". Number of values: " + nValuesToSet)
                .build().buildException();
        }
        
        int nodeIndex = 0;
        // Allocate array based on size of all columns in table,
        // since some values may not be set (if they're nullable).
        final StatementContext context = new StatementContext(upsert, connection, resolver, statement.getParameters(), new Scan());
        UpsertValuesCompiler expressionBuilder = new UpsertValuesCompiler(context);
        final byte[][] values = new byte[nValuesToSet][];
        for (ParseNode valueNode : valueNodes) {
            if (!valueNode.isConstant()) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.VALUE_IN_UPSERT_NOT_CONSTANT).build().buildException();
            }
            PColumn column = allColumns.get(columnIndexes[nodeIndex]);
            expressionBuilder.setColumn(column);
            LiteralExpression literalExpression = (LiteralExpression)valueNode.accept(expressionBuilder);
            byte[] byteValue = literalExpression.getBytes();
            if (literalExpression.getDataType() != null) {
                // If ColumnModifier from expression in SELECT doesn't match the
                // column being projected into then invert the bits.
                if (literalExpression.getColumnModifier() != column.getColumnModifier()) {
                    byte[] tempByteValue = Arrays.copyOf(byteValue, byteValue.length);
                    byteValue = ColumnModifier.SORT_DESC.apply(byteValue, 0, tempByteValue, 0, byteValue.length);
                }
                if (!literalExpression.getDataType().isCoercibleTo(column.getDataType(), literalExpression.getValue())) { 
                    throw new TypeMismatchException(
                        literalExpression.getDataType(), column.getDataType(), "expression: "
                                + literalExpression.toString() + " in column " + column);
                }
                if (!column.getDataType().isSizeCompatible(literalExpression.getDataType(),
                        literalExpression.getValue(), byteValue, literalExpression.getMaxLength(),
                        column.getMaxLength(), literalExpression.getScale(), column.getScale())) { 
                    throw new SQLExceptionInfo.Builder(
                        SQLExceptionCode.DATA_INCOMPATIBLE_WITH_TYPE).setColumnName(column.getName().getString())
                        .setMessage("value=" + literalExpression.toString()).build().buildException();
                }
            }
            byteValue = column.getDataType().coerceBytes(byteValue, literalExpression.getValue(),
                    literalExpression.getDataType(), literalExpression.getMaxLength(), literalExpression.getScale(),
                    column.getMaxLength(), column.getScale());
            values[nodeIndex] = byteValue;
            nodeIndex++;
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
                return new ExplainPlan(Collections.singletonList("PUT SINGLE ROW"));
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
                return LiteralExpression.newConstant(value, column.getDataType(), column.getColumnModifier());
            }
            return super.visit(node);
        }    
        
        @Override
        public Expression visit(LiteralParseNode node) throws SQLException {
            if (isTopLevel()) {
                return LiteralExpression.newConstant(node.getValue(), column.getDataType(), column.getColumnModifier());
            }
            return super.visit(node);
        }
    }
}
