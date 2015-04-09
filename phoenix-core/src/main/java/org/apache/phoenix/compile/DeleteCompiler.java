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
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.cache.ServerCacheClient.ServerCache;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.AggregatePlan;
import org.apache.phoenix.execute.BaseQueryPlan;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.filter.SkipScanFilter;
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
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.MetaDataEntityNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PRow;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.ReadOnlyTableException;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.ScanUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sun.istack.NotNull;

public class DeleteCompiler {
    private static ParseNodeFactory FACTORY = new ParseNodeFactory();
    
    private final PhoenixStatement statement;
    
    public DeleteCompiler(PhoenixStatement statement) {
        this.statement = statement;
    }
    
    private static MutationState deleteRows(PhoenixStatement statement, TableRef targetTableRef, TableRef indexTableRef, ResultIterator iterator, RowProjector projector, TableRef sourceTableRef) throws SQLException {
        PTable table = targetTableRef.getTable();
        PhoenixConnection connection = statement.getConnection();
        PName tenantId = connection.getTenantId();
        byte[] tenantIdBytes = null;
        if (tenantId != null) {
            tenantId = ScanUtil.padTenantIdIfNecessary(table.getRowKeySchema(), table.getBucketNum() != null, tenantId);
            tenantIdBytes = tenantId.getBytes();
        }
        final boolean isAutoCommit = connection.getAutoCommit();
        ConnectionQueryServices services = connection.getQueryServices();
        final int maxSize = services.getProps().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB,QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
        final int batchSize = Math.min(connection.getMutateBatchSize(), maxSize);
        Map<ImmutableBytesPtr,Map<PColumn,byte[]>> mutations = Maps.newHashMapWithExpectedSize(batchSize);
        Map<ImmutableBytesPtr,Map<PColumn,byte[]>> indexMutations = null;
        // If indexTableRef is set, we're deleting the rows from both the index table and
        // the data table through a single query to save executing an additional one.
        if (indexTableRef != null) {
            indexMutations = Maps.newHashMapWithExpectedSize(batchSize);
        }
        try {
            List<PColumn> pkColumns = table.getPKColumns();
            boolean isMultiTenant = table.isMultiTenant() && tenantIdBytes != null;
            boolean isSharedViewIndex = table.getViewIndexId() != null;
            int offset = (table.getBucketNum() == null ? 0 : 1);
            byte[][] values = new byte[pkColumns.size()][];
            if (isMultiTenant) {
                values[offset++] = tenantIdBytes;
            }
            if (isSharedViewIndex) {
                values[offset++] = MetaDataUtil.getViewIndexIdDataType().toBytes(table.getViewIndexId());
            }
            PhoenixResultSet rs = new PhoenixResultSet(iterator, projector, statement);
            int rowCount = 0;
            while (rs.next()) {
                ImmutableBytesPtr ptr = new ImmutableBytesPtr();  // allocate new as this is a key in a Map
                // Use tuple directly, as projector would not have all the PK columns from
                // our index table inside of our projection. Since the tables are equal,
                // there's no transation required.
                if (sourceTableRef.equals(targetTableRef)) {
                    rs.getCurrentRow().getKey(ptr);
                } else {
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
                    table.newKey(ptr, values);
                }
                mutations.put(ptr, PRow.DELETE_MARKER);
                if (indexTableRef != null) {
                    ImmutableBytesPtr indexPtr = new ImmutableBytesPtr(); // allocate new as this is a key in a Map
                    rs.getCurrentRow().getKey(indexPtr);
                    indexMutations.put(indexPtr, PRow.DELETE_MARKER);
                }
                if (mutations.size() > maxSize) {
                    throw new IllegalArgumentException("MutationState size of " + mutations.size() + " is bigger than max allowed size of " + maxSize);
                }
                rowCount++;
                // Commit a batch if auto commit is true and we're at our batch size
                if (isAutoCommit && rowCount % batchSize == 0) {
                    MutationState state = new MutationState(targetTableRef, mutations, 0, maxSize, connection);
                    connection.getMutationState().join(state);
                    if (indexTableRef != null) {
                        MutationState indexState = new MutationState(indexTableRef, indexMutations, 0, maxSize, connection);
                        connection.getMutationState().join(indexState);
                    }
                    connection.commit();
                    mutations.clear();
                    if (indexMutations != null) {
                        indexMutations.clear();
                    }
                }
            }

            // If auto commit is true, this last batch will be committed upon return
            int nCommittedRows = rowCount / batchSize * batchSize;
            MutationState state = new MutationState(targetTableRef, mutations, nCommittedRows, maxSize, connection);
            if (indexTableRef != null) {
                // To prevent the counting of these index rows, we have a negative for remainingRows.
                MutationState indexState = new MutationState(indexTableRef, indexMutations, 0, maxSize, connection);
                state.join(indexState);
            }
            return state;
        } finally {
            iterator.close();
        }
    }
    
    private static class DeletingParallelIteratorFactory extends MutatingParallelIteratorFactory {
        private RowProjector projector;
        private TableRef targetTableRef;
        private TableRef indexTableRef;
        private TableRef sourceTableRef;
        
        private DeletingParallelIteratorFactory(PhoenixConnection connection) {
            super(connection);
        }
        
        @Override
        protected MutationState mutate(StatementContext context, ResultIterator iterator, PhoenixConnection connection) throws SQLException {
            PhoenixStatement statement = new PhoenixStatement(connection);
            return deleteRows(statement, targetTableRef, indexTableRef, iterator, projector, sourceTableRef);
        }
        
        public void setTargetTableRef(TableRef tableRef) {
            this.targetTableRef = tableRef;
        }
        
        public void setSourceTableRef(TableRef tableRef) {
            this.sourceTableRef = tableRef;
        }
        
        public void setRowProjector(RowProjector projector) {
            this.projector = projector;
        }

        public void setIndexTargetTableRef(TableRef indexTableRef) {
            this.indexTableRef = indexTableRef;
        }
        
    }
    
    private Set<PTable> getNonDisabledImmutableIndexes(TableRef tableRef) {
        PTable table = tableRef.getTable();
        if (table.isImmutableRows() && !table.getIndexes().isEmpty()) {
            Set<PTable> nonDisabledIndexes = Sets.newHashSetWithExpectedSize(table.getIndexes().size());
            for (PTable index : table.getIndexes()) {
                if (index.getIndexState() != PIndexState.DISABLE) {
                    nonDisabledIndexes.add(index);
                }
            }
            return nonDisabledIndexes;
        }
        return Collections.emptySet();
    }
    
    private class MultiDeleteMutationPlan implements MutationPlan {
        private final List<MutationPlan> plans;
        private final MutationPlan firstPlan;
        
        public MultiDeleteMutationPlan(@NotNull List<MutationPlan> plans) {
            Preconditions.checkArgument(!plans.isEmpty());
            this.plans = plans;
            this.firstPlan = plans.get(0);
        }
        
        @Override
        public StatementContext getContext() {
            return firstPlan.getContext();
        }

        @Override
        public ParameterMetaData getParameterMetaData() {
            return firstPlan.getParameterMetaData();
        }

        @Override
        public ExplainPlan getExplainPlan() throws SQLException {
            return firstPlan.getExplainPlan();
        }

        @Override
        public PhoenixConnection getConnection() {
            return firstPlan.getConnection();
        }

        @Override
        public MutationState execute() throws SQLException {
            MutationState state = firstPlan.execute();
            for (MutationPlan plan : plans.subList(1, plans.size())) {
                plan.execute();
            }
            return state;
        }
    }
    
    public MutationPlan compile(DeleteStatement delete) throws SQLException {
        final PhoenixConnection connection = statement.getConnection();
        final boolean isAutoCommit = connection.getAutoCommit();
        final boolean hasLimit = delete.getLimit() != null;
        final ConnectionQueryServices services = connection.getQueryServices();
        List<QueryPlan> queryPlans;
        NamedTableNode tableNode = delete.getTable();
        String tableName = tableNode.getName().getTableName();
        String schemaName = tableNode.getName().getSchemaName();
        boolean retryOnce = !isAutoCommit;
        TableRef tableRefToBe;
        boolean noQueryReqd = false;
        boolean runOnServer = false;
        SelectStatement select = null;
        Set<PTable> immutableIndex = Collections.emptySet();
        DeletingParallelIteratorFactory parallelIteratorFactory = null;
        while (true) {
            try {
                ColumnResolver resolver = FromCompiler.getResolverForMutation(delete, connection);
                tableRefToBe = resolver.getTables().get(0);
                PTable table = tableRefToBe.getTable();
                if (table.getType() == PTableType.VIEW && table.getViewType().isReadOnly()) {
                    throw new ReadOnlyTableException(table.getSchemaName().getString(),table.getTableName().getString());
                }
                
                immutableIndex = getNonDisabledImmutableIndexes(tableRefToBe);
                boolean mayHaveImmutableIndexes = !immutableIndex.isEmpty();
                noQueryReqd = !hasLimit;
                runOnServer = isAutoCommit && noQueryReqd;
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
                select = FACTORY.select(
                        delete.getTable(), 
                        hint, false, aliasedNodes, delete.getWhere(), 
                        Collections.<ParseNode>emptyList(), null, 
                        delete.getOrderBy(), delete.getLimit(),
                        delete.getBindCount(), false, false, Collections.<SelectStatement>emptyList());
                select = StatementNormalizer.normalize(select, resolver);
                SelectStatement transformedSelect = SubqueryRewriter.transform(select, resolver, connection);
                if (transformedSelect != select) {
                    resolver = FromCompiler.getResolverForQuery(transformedSelect, connection);
                    select = StatementNormalizer.normalize(transformedSelect, resolver);
                }
                parallelIteratorFactory = hasLimit ? null : new DeletingParallelIteratorFactory(connection);
                QueryOptimizer optimizer = new QueryOptimizer(services);
                queryPlans = Lists.newArrayList(mayHaveImmutableIndexes
                        ? optimizer.getApplicablePlans(statement, select, resolver, Collections.<PColumn>emptyList(), parallelIteratorFactory)
                        : optimizer.getBestPlan(statement, select, resolver, Collections.<PColumn>emptyList(), parallelIteratorFactory));
                if (mayHaveImmutableIndexes) { // FIXME: this is ugly
                    // Lookup the table being deleted from in the cache, as it's possible that the
                    // optimizer updated the cache if it found indexes that were out of date.
                    // If the index was marked as disabled, it should not be in the list
                    // of immutable indexes.
                    table = connection.getMetaDataCache().getTable(new PTableKey(table.getTenantId(), table.getName().getString()));
                    tableRefToBe.setTable(table);
                    immutableIndex = getNonDisabledImmutableIndexes(tableRefToBe);
                }
            } catch (MetaDataEntityNotFoundException e) {
                // Catch column/column family not found exception, as our meta data may
                // be out of sync. Update the cache once and retry if we were out of sync.
                // Otherwise throw, as we'll just get the same error next time.
                if (retryOnce) {
                    retryOnce = false;
                    MetaDataMutationResult result = new MetaDataClient(connection).updateCache(schemaName, tableName);
                    if (result.wasUpdated()) {
                        continue;
                    }
                }
                throw e;
            }
            break;
        }
        final boolean hasImmutableIndexes = !immutableIndex.isEmpty();
        // tableRefs is parallel with queryPlans
        TableRef[] tableRefs = new TableRef[hasImmutableIndexes ? immutableIndex.size() : 1];
        if (hasImmutableIndexes) {
            int i = 0;
            Iterator<QueryPlan> plans = queryPlans.iterator();
            while (plans.hasNext()) {
                QueryPlan plan = plans.next();
                PTable table = plan.getTableRef().getTable();
                if (table.getType() == PTableType.INDEX) { // index plans
                    tableRefs[i++] = plan.getTableRef();
                    immutableIndex.remove(table);
                } else { // data plan
                    /*
                     * If we have immutable indexes that we need to maintain, don't execute the data plan
                     * as we can save a query by piggy-backing on any of the other index queries, since the
                     * PK columns that we need are always in each index row.
                     */
                    plans.remove();
                }
            }
            /*
             * If we have any immutable indexes remaining, then that means that the plan for that index got filtered out
             * because it could not be executed. This would occur if a column in the where clause is not found in the
             * immutable index.
             */
            if (!immutableIndex.isEmpty()) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_FILTER_ON_IMMUTABLE_ROWS).setSchemaName(tableRefToBe.getTable().getSchemaName().getString())
                .setTableName(tableRefToBe.getTable().getTableName().getString()).build().buildException();
            }
        }
        
        // Make sure the first plan is targeting deletion from the data table
        // In the case of an immutable index, we'll also delete from the index.
        tableRefs[0] = tableRefToBe;
        /*
         * Create a mutationPlan for each queryPlan. One plan will be for the deletion of the rows
         * from the data table, while the others will be for deleting rows from immutable indexes.
         */
        List<MutationPlan> mutationPlans = Lists.newArrayListWithExpectedSize(tableRefs.length);
        for (int i = 0; i < tableRefs.length; i++) {
            final TableRef tableRef = tableRefs[i];
            final QueryPlan plan = queryPlans.get(i);
            if (!plan.getTableRef().equals(tableRef) || !(plan instanceof BaseQueryPlan)) {
                runOnServer = false;
                noQueryReqd = false; // FIXME: why set this to false in this case?
            }
            
            final int maxSize = services.getProps().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB,QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
     
            final StatementContext context = plan.getContext();
            // If we're doing a query for a set of rows with no where clause, then we don't need to contact the server at all.
            // A simple check of the none existence of a where clause in the parse node is not sufficient, as the where clause
            // may have been optimized out. Instead, we check that there's a single SkipScanFilter
            if (noQueryReqd
                    && (!context.getScan().hasFilter()
                        || context.getScan().getFilter() instanceof SkipScanFilter)
                    && context.getScanRanges().isPointLookup()) {
                mutationPlans.add(new MutationPlan() {
    
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
                });
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
                mutationPlans.add(new MutationPlan() {
    
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
                        tableRef.getTable().getIndexMaintainers(ptr, context.getConnection());
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
                                final long mutationCount = (Long)projector.getColumnProjector(0).getValue(row, PLong.INSTANCE, ptr);
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
                });
            } else {
                final boolean deleteFromImmutableIndexToo = hasImmutableIndexes && !plan.getTableRef().equals(tableRef);
                if (parallelIteratorFactory != null) {
                    parallelIteratorFactory.setRowProjector(plan.getProjector());
                    parallelIteratorFactory.setTargetTableRef(tableRef);
                    parallelIteratorFactory.setSourceTableRef(plan.getTableRef());
                    parallelIteratorFactory.setIndexTargetTableRef(deleteFromImmutableIndexToo ? plan.getTableRef() : null);
                }
                mutationPlans.add( new MutationPlan() {
    
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
                                totalRowCount += PLong.INSTANCE.getCodec().decodeLong(kv.getValueArray(), kv.getValueOffset(), SortOrder.getDefault());
                            }
                            // Return total number of rows that have been delete. In the case of auto commit being off
                            // the mutations will all be in the mutation state of the current connection.
                            return new MutationState(maxSize, connection, totalRowCount);
                        } else {
                            return deleteRows(statement, tableRef, deleteFromImmutableIndexToo ? plan.getTableRef() : null, iterator, plan.getProjector(), plan.getTableRef());
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
                });
            }
        }
        return mutationPlans.size() == 1 ? mutationPlans.get(0) : new MultiDeleteMutationPlan(mutationPlans);
    }
}
