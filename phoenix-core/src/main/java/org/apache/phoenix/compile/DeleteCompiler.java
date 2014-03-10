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

import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.cache.ServerCacheClient.ServerCache;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.AggregatePlan;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
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
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PRow;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.ReadOnlyTableException;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.MetaDataUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class DeleteCompiler {
    private static ParseNodeFactory FACTORY = new ParseNodeFactory();
    
    private final PhoenixStatement statement;
    
    public DeleteCompiler(PhoenixStatement statement) {
        this.statement = statement;
    }
    
    private static MutationState deleteRows(PhoenixStatement statement, TableRef tableRef, ResultIterator iterator, RowProjector projector) throws SQLException {
        PhoenixConnection connection = statement.getConnection();
        byte[] tenantId = connection.getTenantId() == null ? null : connection.getTenantId().getBytes();
        final boolean isAutoCommit = connection.getAutoCommit();
        ConnectionQueryServices services = connection.getQueryServices();
        final int maxSize = services.getProps().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB,QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
        final int batchSize = Math.min(connection.getMutateBatchSize(), maxSize);
        Map<ImmutableBytesPtr,Map<PColumn,byte[]>> mutations = Maps.newHashMapWithExpectedSize(batchSize);
        try {
            PTable table = tableRef.getTable();
            List<PColumn> pkColumns = table.getPKColumns();
            boolean isMultiTenant = table.isMultiTenant() && tenantId != null;
            boolean isSharedViewIndex = table.getViewIndexId() != null;
            int offset = (table.getBucketNum() == null ? 0 : 1);
            byte[][] values = new byte[pkColumns.size()][];
            if (isMultiTenant) {
                values[offset++] = tenantId;
            }
            if (isSharedViewIndex) {
                values[offset++] = MetaDataUtil.getViewIndexIdDataType().toBytes(table.getViewIndexId());
            }
            ResultSet rs = new PhoenixResultSet(iterator, projector, statement);
            int rowCount = 0;
            while (rs.next()) {
                for (int i = offset; i < values.length; i++) {
                    byte[] byteValue = rs.getBytes(i+1-offset);
                    // The ResultSet.getBytes() call will have inverted it - we need to invert it back.
                    // TODO: consider going under the hood and just getting the bytes
                    if (pkColumns.get(i).getSortOrder() == SortOrder.DESC) {
                        byte[] tempByteValue = Arrays.copyOf(byteValue, byteValue.length);
                        byteValue = SortOrder.invert(byteValue, 0, tempByteValue, 0, byteValue.length);
                    }
                    values[i] = byteValue;
                }
                ImmutableBytesPtr ptr = new ImmutableBytesPtr();
                table.newKey(ptr, values);
                mutations.put(ptr, PRow.DELETE_MARKER);
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
        protected MutationState mutate(StatementContext context, ResultIterator iterator, PhoenixConnection connection) throws SQLException {
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
        final ColumnResolver resolver = FromCompiler.getResolverForMutation(delete, connection);
        final TableRef tableRef = resolver.getTables().get(0);
        final PTable table = tableRef.getTable();
        if (table.getType() == PTableType.VIEW && table.getViewType().isReadOnly()) {
            throw new ReadOnlyTableException(table.getSchemaName().getString(),table.getTableName().getString());
        }
        
        final boolean hasLimit = delete.getLimit() != null;
        boolean noQueryReqd = !hasLimit && !hasImmutableIndex(tableRef);
        boolean runOnServer = isAutoCommit && noQueryReqd;
        HintNode hint = delete.getHint();
        if (runOnServer && !delete.getHint().hasHint(Hint.USE_INDEX_OVER_DATA_TABLE)) {
            hint = HintNode.create(hint, Hint.USE_DATA_OVER_INDEX_TABLE);
        }

        List<AliasedNode> aliasedNodes = Lists.newArrayListWithExpectedSize(table.getPKColumns().size());
        boolean isSalted = table.getBucketNum() != null;
        boolean isMultiTenant = connection.getTenantId() != null && table.isMultiTenant();
        boolean isSharedViewIndex = table.getViewIndexId() != null;
        for (int i = (isSalted ? 1 : 0) + (isMultiTenant ? 1 : 0) + (isSharedViewIndex ? 1 : 0); i < table.getPKColumns().size(); i++) {
            PColumn column = table.getPKColumns().get(i);
            aliasedNodes.add(FACTORY.aliasedNode(null, FACTORY.column(null, '"' + column.getName().getString() + '"', null)));
        }
        SelectStatement select = FACTORY.select(
                Collections.singletonList(delete.getTable()), 
                hint, false, aliasedNodes, delete.getWhere(), 
                Collections.<ParseNode>emptyList(), null, 
                delete.getOrderBy(), delete.getLimit(),
                delete.getBindCount(), false);
        select = StatementNormalizer.normalize(select, resolver);
        DeletingParallelIteratorFactory parallelIteratorFactory = hasLimit ? null : new DeletingParallelIteratorFactory(connection, tableRef);
        final QueryPlan plan = new QueryOptimizer(services).optimize(statement, select, resolver, Collections.<PColumn>emptyList(), parallelIteratorFactory);
        if (!plan.getTableRef().equals(tableRef)) {
            runOnServer = false;
            noQueryReqd = false;
        }
        
        final int maxSize = services.getProps().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB,QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
 
        if (hasImmutableIndexWithKeyValueColumns(tableRef)) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.NO_DELETE_IF_IMMUTABLE_INDEX).setSchemaName(tableRef.getTable().getSchemaName().getString())
            .setTableName(tableRef.getTable().getTableName().getString()).build().buildException();
        }
        
        final StatementContext context = plan.getContext();
        // If we're doing a query for a set of rows with no where clause, then we don't need to contact the server at all.
        // A simple check of the none existence of a where clause in the parse node is not sufficient, as the where clause
        // may have been optimized out. Instead, we check that there's a single filter which must be the SkipScanFilter
        // in this case.
        if (noQueryReqd && ! (context.getScan().getFilter() instanceof FilterList) && context.getScanRanges().isPointLookup()) {
            return new MutationPlan() {

                @Override
                public ParameterMetaData getParameterMetaData() {
                    return context.getBindManager().getParameterMetaData();
                }

                @Override
                public MutationState execute() {
                    // We have a point lookup, so we know we have a simple set of fully qualified
                    // keys for our ranges
                    ScanRanges ranges = context.getScanRanges();
                    Iterator<KeyRange> iterator = ranges.getPointLookupKeyIterator(); 
                    Map<ImmutableBytesPtr,Map<PColumn,byte[]>> mutation = Maps.newHashMapWithExpectedSize(ranges.getPointLookupCount());
                    while (iterator.hasNext()) {
                        mutation.put(new ImmutableBytesPtr(iterator.next().getLowerRange()), PRow.DELETE_MARKER);
                    }
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

                @Override
                public StatementContext getContext() {
                    return context;
                }
            };
        } else if (runOnServer) {
            // TODO: better abstraction
            Scan scan = context.getScan();
            scan.setAttribute(BaseScannerRegionObserver.DELETE_AGG, QueryConstants.TRUE);

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
                public StatementContext getContext() {
                    return context;
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
                        ResultIterator iterator = aggPlan.iterator();
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
                public StatementContext getContext() {
                    return context;
                }

                @Override
                public MutationState execute() throws SQLException {
                    ResultIterator iterator = plan.iterator();
                    if (!hasLimit) {
                        Tuple tuple;
                        long totalRowCount = 0;
                        while ((tuple=iterator.next()) != null) {// Runs query
                            Cell kv = tuple.getValue(0);
                            totalRowCount += PDataType.LONG.getCodec().decodeLong(kv.getValueArray(), kv.getValueOffset(), SortOrder.getDefault());
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
