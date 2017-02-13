/*
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

import static org.apache.phoenix.execute.MutationState.RowTimestampColInfo.NULL_ROWTIMESTAMP_INFO;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.cache.ServerCacheClient;
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
import org.apache.phoenix.execute.MutationState.RowMutationState;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
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
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.ScanUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sun.istack.NotNull;

public class DeleteCompiler {
    private static ParseNodeFactory FACTORY = new ParseNodeFactory();
    
    private final PhoenixStatement statement;
    private final Operation operation;
    
    public DeleteCompiler(PhoenixStatement statement, Operation operation) {
        this.statement = statement;
        this.operation = operation;
    }
    
    private static MutationState deleteRows(StatementContext childContext, TableRef targetTableRef, List<TableRef> indexTableRefs, ResultIterator iterator, RowProjector projector, TableRef sourceTableRef) throws SQLException {
        PTable table = targetTableRef.getTable();
        PhoenixStatement statement = childContext.getStatement();
        PhoenixConnection connection = statement.getConnection();
        PName tenantId = connection.getTenantId();
        byte[] tenantIdBytes = null;
        if (tenantId != null) {
            tenantIdBytes = ScanUtil.getTenantIdBytes(table.getRowKeySchema(), table.getBucketNum() != null, tenantId, table.getViewIndexId() != null);
        }
        final boolean isAutoCommit = connection.getAutoCommit();
        ConnectionQueryServices services = connection.getQueryServices();
        final int maxSize = services.getProps().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB,QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
        final int batchSize = Math.min(connection.getMutateBatchSize(), maxSize);
        Map<ImmutableBytesPtr,RowMutationState> mutations = Maps.newHashMapWithExpectedSize(batchSize);
        List<Map<ImmutableBytesPtr,RowMutationState>> indexMutations = null;
        // If indexTableRef is set, we're deleting the rows from both the index table and
        // the data table through a single query to save executing an additional one.
        if (!indexTableRefs.isEmpty()) {
            indexMutations = Lists.newArrayListWithExpectedSize(indexTableRefs.size());
            for (int i = 0; i < indexTableRefs.size(); i++) {
                indexMutations.add(Maps.<ImmutableBytesPtr,RowMutationState>newHashMapWithExpectedSize(batchSize));
            }
        }
        List<PColumn> pkColumns = table.getPKColumns();
        boolean isMultiTenant = table.isMultiTenant() && tenantIdBytes != null;
        boolean isSharedViewIndex = table.getViewIndexId() != null;
        int offset = (table.getBucketNum() == null ? 0 : 1);
        byte[][] values = new byte[pkColumns.size()][];
        if (isSharedViewIndex) {
            values[offset++] = MetaDataUtil.getViewIndexIdDataType().toBytes(table.getViewIndexId());
        }
        if (isMultiTenant) {
            values[offset++] = tenantIdBytes;
        }
        try (PhoenixResultSet rs = new PhoenixResultSet(iterator, projector, childContext)) {
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
                // When issuing deletes, we do not care about the row time ranges. Also, if the table had a row timestamp column, then the
                // row key will already have its value. 
                mutations.put(ptr, new RowMutationState(PRow.DELETE_MARKER, statement.getConnection().getStatementExecutionCounter(), NULL_ROWTIMESTAMP_INFO, null));
                for (int i = 0; i < indexTableRefs.size(); i++) {
                    ImmutableBytesPtr indexPtr = new ImmutableBytesPtr(); // allocate new as this is a key in a Map
                    rs.getCurrentRow().getKey(indexPtr);
                    indexMutations.get(i).put(indexPtr, new RowMutationState(PRow.DELETE_MARKER, statement.getConnection().getStatementExecutionCounter(), NULL_ROWTIMESTAMP_INFO, null));
                }
                if (mutations.size() > maxSize) {
                    throw new IllegalArgumentException("MutationState size of " + mutations.size() + " is bigger than max allowed size of " + maxSize);
                }
                rowCount++;
                // Commit a batch if auto commit is true and we're at our batch size
                if (isAutoCommit && rowCount % batchSize == 0) {
                    MutationState state = new MutationState(targetTableRef, mutations, 0, maxSize, connection);
                    connection.getMutationState().join(state);
                    for (int i = 0; i < indexTableRefs.size(); i++) {
                        MutationState indexState = new MutationState(indexTableRefs.get(i), indexMutations.get(i), 0, maxSize, connection);
                        connection.getMutationState().join(indexState);
                    }
                    connection.getMutationState().send();
                    mutations.clear();
                    if (indexMutations != null) {
                        indexMutations.clear();
                    }
                }
            }

            // If auto commit is true, this last batch will be committed upon return
            int nCommittedRows = isAutoCommit ? (rowCount / batchSize * batchSize) : 0;
            MutationState state = new MutationState(targetTableRef, mutations, nCommittedRows, maxSize, connection);
            for (int i = 0; i < indexTableRefs.size(); i++) {
                // To prevent the counting of these index rows, we have a negative for remainingRows.
                MutationState indexState = new MutationState(indexTableRefs.get(i), indexMutations.get(i), 0, maxSize, connection);
                state.join(indexState);
            }
            return state;
        }
    }
    
    private static class DeletingParallelIteratorFactory extends MutatingParallelIteratorFactory {
        private RowProjector projector;
        private TableRef targetTableRef;
        private List<TableRef> indexTableRefs;
        private TableRef sourceTableRef;
        
        private DeletingParallelIteratorFactory(PhoenixConnection connection) {
            super(connection);
        }
        
        @Override
        protected MutationState mutate(StatementContext parentContext, ResultIterator iterator, PhoenixConnection connection) throws SQLException {
            PhoenixStatement statement = new PhoenixStatement(connection);
            /*
             * We don't want to collect any read metrics within the child context. This is because any read metrics that
             * need to be captured are already getting collected in the parent statement context enclosed in the result
             * iterator being used for reading rows out.
             */
            StatementContext ctx = new StatementContext(statement, false);
            MutationState state = deleteRows(ctx, targetTableRef, indexTableRefs, iterator, projector, sourceTableRef);
            return state;
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

        public void setIndexTargetTableRefs(List<TableRef> indexTableRefs) {
            this.indexTableRefs = indexTableRefs;
        }
        
    }
    
    private Map<PTableKey, PTable> getNonDisabledImmutableIndexes(TableRef tableRef) {
        PTable table = tableRef.getTable();
        if (table.isImmutableRows() && !table.getIndexes().isEmpty()) {
            Map<PTableKey, PTable> nonDisabledIndexes = new HashMap<PTableKey, PTable>(table.getIndexes().size());
            for (PTable index : table.getIndexes()) {
                if (index.getIndexState() != PIndexState.DISABLE) {
                    nonDisabledIndexes.put(index.getKey(), index);
                }
            }
            return nonDisabledIndexes;
        }
        return Collections.emptyMap();
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
        public MutationState execute() throws SQLException {
            MutationState state = firstPlan.execute();
            for (MutationPlan plan : plans.subList(1, plans.size())) {
                plan.execute();
            }
            return state;
        }

        @Override
        public TableRef getTargetRef() {
            return firstPlan.getTargetRef();
        }

        @Override
        public Set<TableRef> getSourceRefs() {
            return firstPlan.getSourceRefs();
        }

		@Override
		public Operation getOperation() {
			return operation;
		}
    }
    
    private static boolean hasNonPKIndexedColumns(Collection<PTable> immutableIndexes) {
        for (PTable index : immutableIndexes) {
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
        ColumnResolver resolverToBe = null;
        Map<PTableKey, PTable> immutableIndex = Collections.emptyMap();
        DeletingParallelIteratorFactory parallelIteratorFactory;
        QueryPlan dataPlanToBe = null;
        while (true) {
            try {
                resolverToBe = FromCompiler.getResolverForMutation(delete, connection);
                tableRefToBe = resolverToBe.getTables().get(0);
                PTable table = tableRefToBe.getTable();
                // Cannot update:
                // - read-only VIEW 
                // - transactional table with a connection having an SCN
                // TODO: SchemaUtil.isReadOnly(PTable, connection)?
                if (table.getType() == PTableType.VIEW && table.getViewType().isReadOnly()) {
                    throw new ReadOnlyTableException(schemaName,tableName);
                }
                else if (table.isTransactional() && connection.getSCN() != null) {
                   throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_SPECIFY_SCN_FOR_TXN_TABLE).setSchemaName(schemaName)
                   .setTableName(tableName).build().buildException();
                }
                
                immutableIndex = getNonDisabledImmutableIndexes(tableRefToBe);
                boolean mayHaveImmutableIndexes = !immutableIndex.isEmpty();
                noQueryReqd = !hasLimit;
                // Can't run on same server for transactional data, as we need the row keys for the data
                // that is being upserted for conflict detection purposes.
                runOnServer = isAutoCommit && noQueryReqd && !table.isTransactional();
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
                select = FACTORY.select(delete.getTable(), hint, false, aliasedNodes, delete.getWhere(),
                        Collections.<ParseNode> emptyList(), null, delete.getOrderBy(), delete.getLimit(), null,
                        delete.getBindCount(), false, false, Collections.<SelectStatement> emptyList(),
                        delete.getUdfParseNodes());
                select = StatementNormalizer.normalize(select, resolverToBe);
                SelectStatement transformedSelect = SubqueryRewriter.transform(select, resolverToBe, connection);
                if (transformedSelect != select) {
                    resolverToBe = FromCompiler.getResolverForQuery(transformedSelect, connection);
                    select = StatementNormalizer.normalize(transformedSelect, resolverToBe);
                }
                parallelIteratorFactory = hasLimit ? null : new DeletingParallelIteratorFactory(connection);
                QueryOptimizer optimizer = new QueryOptimizer(services);
                QueryCompiler compiler = new QueryCompiler(statement, select, resolverToBe, Collections.<PColumn>emptyList(), parallelIteratorFactory, new SequenceManager(statement));
                dataPlanToBe = compiler.compile();
                queryPlans = Lists.newArrayList(mayHaveImmutableIndexes
                        ? optimizer.getApplicablePlans(dataPlanToBe, statement, select, resolverToBe, Collections.<PColumn>emptyList(), parallelIteratorFactory)
                        : optimizer.getBestPlan(dataPlanToBe, statement, select, resolverToBe, Collections.<PColumn>emptyList(), parallelIteratorFactory));
                if (mayHaveImmutableIndexes) { // FIXME: this is ugly
                    // Lookup the table being deleted from in the cache, as it's possible that the
                    // optimizer updated the cache if it found indexes that were out of date.
                    // If the index was marked as disabled, it should not be in the list
                    // of immutable indexes.
                    table = connection.getTable(new PTableKey(table.getTenantId(), table.getName().getString()));
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
        boolean isBuildingImmutable = false;
        final boolean hasImmutableIndexes = !immutableIndex.isEmpty();
        if (hasImmutableIndexes) {
            for (PTable index : immutableIndex.values()){
                if (index.getIndexState() == PIndexState.BUILDING) {
                    isBuildingImmutable = true;
                    break;
                }
            }
        }
        final QueryPlan dataPlan = dataPlanToBe;
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
                    immutableIndex.remove(table.getKey());
                } else if (!isBuildingImmutable) { // data plan
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
                Collection<PTable> immutableIndexes = immutableIndex.values();
                if (!isBuildingImmutable || hasNonPKIndexedColumns(immutableIndexes)) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_FILTER_ON_IMMUTABLE_ROWS).setSchemaName(tableRefToBe.getTable().getSchemaName().getString())
                    .setTableName(tableRefToBe.getTable().getTableName().getString()).build().buildException();
                }
                runOnServer = false;
            }
        }
        List<TableRef> buildingImmutableIndexes = Lists.newArrayListWithExpectedSize(immutableIndex.values().size());
        for (PTable index : immutableIndex.values()) {
            buildingImmutableIndexes.add(new TableRef(index, dataPlan.getTableRef().getTimeStamp(), dataPlan.getTableRef().getLowerBoundTimeStamp()));
        }
        
        // Make sure the first plan is targeting deletion from the data table
        // In the case of an immutable index, we'll also delete from the index.
        final TableRef dataTableRef = tableRefs[0] = tableRefToBe;
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
                    public MutationState execute() throws SQLException {
                        // We have a point lookup, so we know we have a simple set of fully qualified
                        // keys for our ranges
                        ScanRanges ranges = context.getScanRanges();
                        Iterator<KeyRange> iterator = ranges.getPointLookupKeyIterator(); 
                        Map<ImmutableBytesPtr,RowMutationState> mutation = Maps.newHashMapWithExpectedSize(ranges.getPointLookupCount());
                        while (iterator.hasNext()) {
                            mutation.put(new ImmutableBytesPtr(iterator.next().getLowerRange()), new RowMutationState(PRow.DELETE_MARKER, statement.getConnection().getStatementExecutionCounter(), NULL_ROWTIMESTAMP_INFO, null));
                        }
                        return new MutationState(tableRef, mutation, 0, maxSize, connection);
                    }
    
                    @Override
                    public ExplainPlan getExplainPlan() throws SQLException {
                        return new ExplainPlan(Collections.singletonList("DELETE SINGLE ROW"));
                    }
    
                    @Override
                    public StatementContext getContext() {
                        return context;
                    }

                    @Override
                    public TableRef getTargetRef() {
                        return dataTableRef;
                    }

                    @Override
                    public Set<TableRef> getSourceRefs() {
                        // Don't include the target
                        return Collections.emptySet();
                    }

            		@Override
            		public Operation getOperation() {
            			return operation;
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
                RowProjector projectorToBe = ProjectionCompiler.compile(context, aggSelect, GroupBy.EMPTY_GROUP_BY);
                context.getAggregationManager().compile(context, GroupBy.EMPTY_GROUP_BY);
                if (plan.getProjector().projectEveryRow()) {
                    projectorToBe = new RowProjector(projectorToBe,true);
                }
                final RowProjector projector = projectorToBe;
                final QueryPlan aggPlan = new AggregatePlan(context, select, tableRef, projector, null, null,
                        OrderBy.EMPTY_ORDER_BY, null, GroupBy.EMPTY_GROUP_BY, null);
                mutationPlans.add(new MutationPlan() {
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
                        return dataTableRef;
                    }

                    @Override
                    public Set<TableRef> getSourceRefs() {
                        return dataPlan.getSourceRefs();
                    }

            		@Override
            		public Operation getOperation() {
            			return operation;
            		}

                    @Override
                    public MutationState execute() throws SQLException {
                        // TODO: share this block of code with UPSERT SELECT
                        ImmutableBytesWritable ptr = context.getTempPtr();
                        PTable table = tableRef.getTable();
                        table.getIndexMaintainers(ptr, context.getConnection());
                        byte[] txState = table.isTransactional() ? connection.getMutationState().encodeTransaction() : ByteUtil.EMPTY_BYTE_ARRAY;
                        ServerCache cache = null;
                        try {
                            if (ptr.getLength() > 0) {
                                byte[] uuidValue = ServerCacheClient.generateId();
                                context.getScan().setAttribute(PhoenixIndexCodec.INDEX_UUID, uuidValue);
                                context.getScan().setAttribute(PhoenixIndexCodec.INDEX_PROTO_MD, ptr.get());
                                context.getScan().setAttribute(BaseScannerRegionObserver.TX_STATE, txState);
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
                List<TableRef> immutableIndexRefsToBe = Lists.newArrayListWithExpectedSize(dataPlan.getTableRef().getTable().getIndexes().size());
                if (!buildingImmutableIndexes.isEmpty()) {
                    immutableIndexRefsToBe = buildingImmutableIndexes;
                } else if (hasImmutableIndexes && !plan.getTableRef().equals(tableRef)) {
                    immutableIndexRefsToBe = Collections.singletonList(plan.getTableRef());
                }
                final List<TableRef> immutableIndexRefs = immutableIndexRefsToBe;
                final DeletingParallelIteratorFactory parallelIteratorFactory2 = parallelIteratorFactory;
                mutationPlans.add( new MutationPlan() {
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
                        return dataTableRef;
                    }

                    @Override
                    public Set<TableRef> getSourceRefs() {
                        return dataPlan.getSourceRefs();
                    }

            		@Override
            		public Operation getOperation() {
            			return operation;
            		}

                    @Override
                    public MutationState execute() throws SQLException {
                        ResultIterator iterator = plan.iterator();
                        try {
                            if (!hasLimit) {
                                Tuple tuple;
                                long totalRowCount = 0;
                                if (parallelIteratorFactory2 != null) {
                                    parallelIteratorFactory2.setRowProjector(plan.getProjector());
                                    parallelIteratorFactory2.setTargetTableRef(tableRef);
                                    parallelIteratorFactory2.setSourceTableRef(plan.getTableRef());
                                    parallelIteratorFactory2.setIndexTargetTableRefs(immutableIndexRefs);
                                }
                                while ((tuple=iterator.next()) != null) {// Runs query
                                    Cell kv = tuple.getValue(0);
                                    totalRowCount += PLong.INSTANCE.getCodec().decodeLong(kv.getValueArray(), kv.getValueOffset(), SortOrder.getDefault());
                                }
                                // Return total number of rows that have been delete. In the case of auto commit being off
                                // the mutations will all be in the mutation state of the current connection.
                                MutationState state = new MutationState(maxSize, connection, totalRowCount);

                                // set the read metrics accumulated in the parent context so that it can be published when the mutations are committed.
                                state.setReadMetricQueue(plan.getContext().getReadMetricsQueue());

                                return state;
                            } else {
                                return deleteRows(plan.getContext(), tableRef, immutableIndexRefs, iterator, plan.getProjector(), plan.getTableRef());
                            }
                        } finally {
                            iterator.close();
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