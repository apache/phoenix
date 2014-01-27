/*
 * Copyright 2010 The Apache Software Foundation
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
import org.apache.phoenix.index.IndexMetaDataCacheClient;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.optimize.QueryOptimizer;
import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.parse.DeleteStatement;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.query.Scanner;
import org.apache.phoenix.schema.ColumnModifier;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.ReadOnlyTableException;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.IndexUtil;

public class DeleteCompiler {
    private static ParseNodeFactory FACTORY = new ParseNodeFactory();
    
    private final PhoenixStatement statement;
    
    public DeleteCompiler(PhoenixStatement statement) {
        this.statement = statement;
    }
    
    private static MutationState deleteRows(PhoenixStatement statement, TableRef tableRef, ResultIterator iterator, RowProjector projector) throws SQLException {
        PhoenixConnection connection = statement.getConnection();
        final boolean isAutoCommit = connection.getAutoCommit();
        ConnectionQueryServices services = connection.getQueryServices();
        final int maxSize = services.getProps().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB,QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
        final int batchSize = Math.min(connection.getMutateBatchSize(), maxSize);
        Map<ImmutableBytesPtr,Map<PColumn,byte[]>> mutations = Maps.newHashMapWithExpectedSize(batchSize);
        try {
            PTable table = tableRef.getTable();
            List<PColumn> pkColumns = table.getPKColumns();
            int offset = table.getBucketNum() == null ? 0 : 1; // Take into account salting
            byte[][] values = new byte[pkColumns.size()][];
            ResultSet rs = new PhoenixResultSet(iterator, projector, statement);
            int rowCount = 0;
            while (rs.next()) {
                for (int i = offset; i < values.length; i++) {
                    byte[] byteValue = rs.getBytes(i+1-offset);
                    // The ResultSet.getBytes() call will have inverted it - we need to invert it back.
                    // TODO: consider going under the hood and just getting the bytes
                    if (pkColumns.get(i).getColumnModifier() == ColumnModifier.SORT_DESC) {
                        byte[] tempByteValue = Arrays.copyOf(byteValue, byteValue.length);
                        byteValue = ColumnModifier.SORT_DESC.apply(byteValue, 0, tempByteValue, 0, byteValue.length);
                    }
                    values[i] = byteValue;
                }
                ImmutableBytesPtr ptr = new ImmutableBytesPtr();
                table.newKey(ptr, values);
                mutations.put(ptr, null);
                if (mutations.size() > maxSize) {
                    throw new IllegalArgumentException("MutationState size of " + mutations.size() + " is bigger than max allowed size of " + maxSize);
                }
                rowCount++;
                // Commit a batch if auto commit is true and we're at our batch size
                if (isAutoCommit && rowCount % batchSize == 0) {
                    MutationState state = new MutationState(tableRef, mutations, 0, maxSize, connection);
                    connection.getMutationState().join(state);
                    connection.commit();
                    mutations.clear();
                }
            }

            // If auto commit is true, this last batch will be committed upon return
            return new MutationState(tableRef, mutations, rowCount / batchSize * batchSize, maxSize, connection);
        } finally {
            iterator.close();
        }
    }
    
    private static class DeletingParallelIteratorFactory extends MutatingParallelIteratorFactory {
        private RowProjector projector;
        
        private DeletingParallelIteratorFactory(PhoenixConnection connection, TableRef tableRef) {
            super(connection, tableRef);
        }
        
        @Override
        protected MutationState mutate(PhoenixConnection connection, ResultIterator iterator) throws SQLException {
            PhoenixStatement statement = new PhoenixStatement(connection);
            return deleteRows(statement, tableRef, iterator, projector);
        }
        
        public void setRowProjector(RowProjector projector) {
            this.projector = projector;
        }
        
    }
    
    private boolean hasImmutableIndex(TableRef tableRef) {
        return tableRef.getTable().isImmutableRows() && !tableRef.getTable().getIndexes().isEmpty();
    }
    
    private boolean hasImmutableIndexWithKeyValueColumns(TableRef tableRef) {
        if (!hasImmutableIndex(tableRef)) {
            return false;
        }
        for (PTable index : tableRef.getTable().getIndexes()) {
            for (PColumn column : index.getPKColumns()) {
                if (!IndexUtil.isDataPKColumn(column)) {
                    return true;
                }
            }
        }
        return false;
    }
    
    public MutationPlan compile(DeleteStatement delete) throws SQLException {
        final PhoenixConnection connection = statement.getConnection();
        final boolean isAutoCommit = connection.getAutoCommit();
        final ConnectionQueryServices services = connection.getQueryServices();
        final ColumnResolver resolver = FromCompiler.getResolver(delete, connection);
        final TableRef tableRef = resolver.getTables().get(0);
        if (tableRef.getTable().getType() == PTableType.VIEW) {
            throw new ReadOnlyTableException("Mutations not allowed for a view (" + tableRef.getTable() + ")");
        }
        
        final boolean hasLimit = delete.getLimit() != null;
        boolean runOnServer = isAutoCommit && !hasLimit && !hasImmutableIndex(tableRef);
        HintNode hint = delete.getHint();
        if (runOnServer && !delete.getHint().hasHint(Hint.USE_INDEX_OVER_DATA_TABLE)) {
            hint = HintNode.create(hint, Hint.USE_DATA_OVER_INDEX_TABLE);
        }

        PTable table = tableRef.getTable();
        List<AliasedNode> aliasedNodes = Lists.newArrayListWithExpectedSize(table.getPKColumns().size());
        for (int i = table.getBucketNum() == null ? 0 : 1; i < table.getPKColumns().size(); i++) {
            PColumn column = table.getPKColumns().get(i);
            String name = column.getName().getString();
            aliasedNodes.add(FACTORY.aliasedNode(null, FACTORY.column(null, name, name)));
        }
        SelectStatement select = FACTORY.select(
                Collections.singletonList(delete.getTable()), 
                hint, false, aliasedNodes, delete.getWhere(), 
                Collections.<ParseNode>emptyList(), null, 
                delete.getOrderBy(), delete.getLimit(),
                delete.getBindCount(), false);
        DeletingParallelIteratorFactory parallelIteratorFactory = hasLimit ? null : new DeletingParallelIteratorFactory(connection, tableRef);
        final QueryPlan plan = new QueryOptimizer(services).optimize(select, statement, Collections.<PColumn>emptyList(), parallelIteratorFactory);
        runOnServer &= plan.getTableRef().equals(tableRef);
        
        final int maxSize = services.getProps().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB,QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
 
        if (hasImmutableIndexWithKeyValueColumns(tableRef)) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.NO_DELETE_IF_IMMUTABLE_INDEX).setSchemaName(tableRef.getTable().getSchemaName().getString())
            .setTableName(tableRef.getTable().getTableName().getString()).build().buildException();
        }
        
        final StatementContext context = plan.getContext();
        // If we're doing a query for a single row with no where clause, then we don't need to contact the server at all.
        // A simple check of the none existence of a where clause in the parse node is not sufficient, as the where clause
        // may have been optimized out.
        if (runOnServer && context.isSingleRowScan()) {
            final ImmutableBytesPtr key = new ImmutableBytesPtr(context.getScan().getStartRow());
            return new MutationPlan() {

                @Override
                public ParameterMetaData getParameterMetaData() {
                    return context.getBindManager().getParameterMetaData();
                }

                @Override
                public MutationState execute() {
                    Map<ImmutableBytesPtr,Map<PColumn,byte[]>> mutation = Maps.newHashMapWithExpectedSize(1);
                    mutation.put(key, null);
                    return new MutationState(tableRef, mutation, 0, maxSize, connection);
                }

                @Override
                public ExplainPlan getExplainPlan() throws SQLException {
                    return new ExplainPlan(Collections.singletonList("DELETE SINGLE ROW"));
                }

                @Override
                public PhoenixConnection getConnection() {
                    return connection;
                }
            };
        } else if (runOnServer) {
            // TODO: better abstraction
            Scan scan = context.getScan();
            scan.setAttribute(UngroupedAggregateRegionObserver.DELETE_AGG, QueryConstants.TRUE);

            // Build an ungrouped aggregate query: select COUNT(*) from <table> where <where>
            // The coprocessor will delete each row returned from the scan
            // Ignoring ORDER BY, since with auto commit on and no limit makes no difference
            SelectStatement aggSelect = SelectStatement.create(SelectStatement.COUNT_ONE, delete.getHint());
            final RowProjector projector = ProjectionCompiler.compile(context, aggSelect, GroupBy.EMPTY_GROUP_BY);
            final QueryPlan aggPlan = new AggregatePlan(context, select, tableRef, projector, null, OrderBy.EMPTY_ORDER_BY, null, GroupBy.EMPTY_GROUP_BY, null);
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
                public MutationState execute() throws SQLException {
                    // TODO: share this block of code with UPSERT SELECT
                    ImmutableBytesWritable ptr = context.getTempPtr();
                    tableRef.getTable().getIndexMaintainers(ptr);
                    ServerCache cache = null;
                    try {
                        if (ptr.getLength() > 0) {
                            IndexMetaDataCacheClient client = new IndexMetaDataCacheClient(connection, tableRef);
                            cache = client.addIndexMetadataCache(context.getScanRanges(), ptr);
                            byte[] uuidValue = cache.getId();
                            context.getScan().setAttribute(PhoenixIndexCodec.INDEX_UUID, uuidValue);
                        }
                        Scanner scanner = aggPlan.getScanner();
                        ResultIterator iterator = scanner.iterator();
                        try {
                            Tuple row = iterator.next();
                            final long mutationCount = (Long)projector.getColumnProjector(0).getValue(row, PDataType.LONG, ptr);
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
                    planSteps.add("DELETE ROWS");
                    planSteps.addAll(queryPlanSteps);
                    return new ExplainPlan(planSteps);
                }
            };
        } else {
            if (parallelIteratorFactory != null) {
                parallelIteratorFactory.setRowProjector(plan.getProjector());
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
                public MutationState execute() throws SQLException {
                    Scanner scanner = plan.getScanner();
                    ResultIterator iterator = scanner.iterator();
                    if (!hasLimit) {
                        Tuple tuple;
                        long totalRowCount = 0;
                        while ((tuple=iterator.next()) != null) {// Runs query
                            KeyValue kv = tuple.getValue(0);
                            totalRowCount += PDataType.LONG.getCodec().decodeLong(kv.getBuffer(), kv.getValueOffset(), null);
                        }
                        // Return total number of rows that have been delete. In the case of auto commit being off
                        // the mutations will all be in the mutation state of the current connection.
                        return new MutationState(maxSize, connection, totalRowCount);
                    } else {
                        return deleteRows(statement, tableRef, iterator, plan.getProjector());
                    }
                }

                @Override
                public ExplainPlan getExplainPlan() throws SQLException {
                    List<String> queryPlanSteps =  plan.getExplainPlan().getPlanSteps();
                    List<String> planSteps = Lists.newArrayListWithExpectedSize(queryPlanSteps.size()+1);
                    planSteps.add("DELETE ROWS");
                    planSteps.addAll(queryPlanSteps);
                    return new ExplainPlan(planSteps);
                }
            };
        }
       
    }
}
