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
import static org.apache.phoenix.util.NumberUtil.add;

import java.io.IOException;
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.cache.ServerCacheClient;
import org.apache.phoenix.cache.ServerCacheClient.ServerCache;
import org.apache.phoenix.compile.ExplainPlanAttributes
    .ExplainPlanAttributesBuilder;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.AggregatePlan;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.execute.MutationState.MultiRowMutationState;
import org.apache.phoenix.execute.MutationState.RowMutationState;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.hbase.index.AbstractValueGetter;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
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
import org.apache.phoenix.parse.DeleteStatement;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.DelegateColumn;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PRow;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.ReadOnlyTableException;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.transaction.PhoenixTransactionProvider.Feature;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ScanUtil;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.NonNull;
import org.apache.phoenix.util.SchemaUtil;

public class DeleteCompiler {
    private static ParseNodeFactory FACTORY = new ParseNodeFactory();
    
    private final PhoenixStatement statement;
    private final Operation operation;
    
    public DeleteCompiler(PhoenixStatement statement, Operation operation) {
        this.statement = statement;
        this.operation = operation;
    }
    
    /**
     * Handles client side deletion of rows for a DELETE statement. We determine the "best" plan to drive the query using
     * our standard optimizer. The plan may be based on using an index, in which case we need to translate the index row
     * key to get the data row key used to form the delete mutation. We always collect up the data table mutations, but we
     * only collect and send the index mutations for global, immutable indexes. Local indexes and mutable indexes are always
     * maintained on the server side.
     * @param context StatementContext for the scan being executed
     * @param iterator ResultIterator for the scan being executed
     * @param bestPlan QueryPlan used to produce the iterator
     * @param projectedTableRef TableRef containing all indexed and covered columns across all indexes on the data table
     * @param otherTableRefs other TableRefs needed to be maintained apart from the one over which the scan is executing.
     *  Might be other index tables (if we're driving off of the data table table), the data table (if we're driving off of
     *  an index table), or a mix of the data table and additional index tables.
     * @return MutationState representing the uncommitted data across the data table and indexes. Will be joined with the
     *  MutationState on the connection over which the delete is occurring.
     * @throws SQLException
     */
    private static MutationState deleteRows(StatementContext context, ResultIterator iterator, QueryPlan bestPlan, TableRef projectedTableRef, List<TableRef> otherTableRefs) throws SQLException {
        RowProjector projector = bestPlan.getProjector();
        TableRef tableRef = bestPlan.getTableRef();
        PTable table = tableRef.getTable();
        PhoenixStatement statement = context.getStatement();
        PhoenixConnection connection = statement.getConnection();
        PName tenantId = connection.getTenantId();
        byte[] tenantIdBytes = null;
        if (tenantId != null) {
            tenantIdBytes = ScanUtil.getTenantIdBytes(table.getRowKeySchema(), table.getBucketNum() != null, tenantId, table.getViewIndexId() != null);
        }
        // we automatically flush the mutations when either auto commit is enabled, or
        // the target table is transactional (in that case changes are not visible until we commit)
        final boolean autoFlush = connection.getAutoCommit() || tableRef.getTable().isTransactional();
        ConnectionQueryServices services = connection.getQueryServices();
        final int maxSize = services.getProps().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB,QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
        final long maxSizeBytes = services.getProps()
                .getLongBytes(QueryServices.MAX_MUTATION_SIZE_BYTES_ATTRIB,
                        QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE_BYTES);
        final int batchSize = Math.min(connection.getMutateBatchSize(), maxSize);
        MultiRowMutationState mutations = new MultiRowMutationState(batchSize);
        List<MultiRowMutationState> otherMutations = null;
        // If otherTableRefs is not empty, we're deleting the rows from both the index table and
        // the data table through a single query to save executing an additional one (since we
        // can always get the data table row key from an index row key).
        if (!otherTableRefs.isEmpty()) {
            otherMutations = Lists.newArrayListWithExpectedSize(otherTableRefs.size());
            for (int i = 0; i < otherTableRefs.size(); i++) {
                otherMutations.add(new MultiRowMutationState(batchSize));
            }
        }
        List<PColumn> pkColumns = table.getPKColumns();
        boolean isMultiTenant = table.isMultiTenant() && tenantIdBytes != null;
        boolean isSharedViewIndex = table.getViewIndexId() != null;
        int offset = (table.getBucketNum() == null ? 0 : 1);
        byte[][] values = new byte[pkColumns.size()][];
        if (isSharedViewIndex) {
            values[offset++] = table.getviewIndexIdType().toBytes(table.getViewIndexId());
        }
        if (isMultiTenant) {
            values[offset++] = tenantIdBytes;
        }
        try (final PhoenixResultSet rs = new PhoenixResultSet(iterator, projector, context)) {
            ValueGetter getter = null;
            if (!otherTableRefs.isEmpty()) {
                getter = new AbstractValueGetter() {
                    final ImmutableBytesWritable valuePtr = new ImmutableBytesWritable();
                    final ImmutableBytesWritable rowKeyPtr = new ImmutableBytesWritable();
    
                    @Override
                    public ImmutableBytesWritable getLatestValue(ColumnReference ref, long ts) throws IOException {
                        Cell cell = rs.getCurrentRow().getValue(ref.getFamily(), ref.getQualifier());
                        if (cell == null) {
                            return null;
                        }
                        valuePtr.set(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                        return valuePtr;
                    }
    
                    @Override
                    public byte[] getRowKey() {
                        rs.getCurrentRow().getKey(rowKeyPtr);
                        return ByteUtil.copyKeyBytesIfNecessary(rowKeyPtr);
                    }
                };
            }
            IndexMaintainer scannedIndexMaintainer = null;
            IndexMaintainer[] maintainers = null;
            PTable dataTable = table;
            if (table.getType() == PTableType.INDEX) {
                if (!otherTableRefs.isEmpty()) {
                    // The data table is always the last one in the list if it's
                    // not chosen as the best of the possible plans.
                    dataTable = otherTableRefs.get(otherTableRefs.size()-1).getTable();
                    if (!isMaintainedOnClient(table)) {
                        // dataTable is a projected table and may not include all the indexed columns and so we need to get
                        // the actual data table
                        dataTable = connection.getTable(SchemaUtil
                                .getTableName(dataTable.getSchemaName().getString(),
                                        dataTable.getTableName().getString()));
                    }
                    scannedIndexMaintainer = IndexMaintainer.create(dataTable, table, connection);
                }
                maintainers = new IndexMaintainer[otherTableRefs.size()];
                for (int i = 0; i < otherTableRefs.size(); i++) {
                    // Create IndexMaintainer based on projected table (i.e. SELECT expressions) so that client-side
                    // expressions are used instead of server-side ones.
                    PTable otherTable = otherTableRefs.get(i).getTable();
                    if (otherTable.getType() == PTableType.INDEX) {
                        // In this case, we'll convert from index row -> data row -> other index row
                        maintainers[i] = IndexMaintainer.create(dataTable, otherTable, connection);
                    } else {
                        maintainers[i] = scannedIndexMaintainer;
                    }
                }
            } else if (!otherTableRefs.isEmpty()) {
                dataTable = table;
                maintainers = new IndexMaintainer[otherTableRefs.size()];
                for (int i = 0; i < otherTableRefs.size(); i++) {
                    // Create IndexMaintainer based on projected table (i.e. SELECT expressions) so that client-side
                    // expressions are used instead of server-side ones.
                    maintainers[i] = IndexMaintainer.create(projectedTableRef.getTable(), otherTableRefs.get(i).getTable(), connection);
                }

            }
            byte[][] viewConstants = IndexUtil.getViewConstants(dataTable);
            int rowCount = 0;
            while (rs.next()) {
                ImmutableBytesPtr rowKeyPtr = new ImmutableBytesPtr();  // allocate new as this is a key in a Map
                rs.getCurrentRow().getKey(rowKeyPtr);
                // When issuing deletes, we do not care about the row time ranges. Also, if the table had a row timestamp column, then the
                // row key will already have its value.
                // Check for otherTableRefs being empty required when deleting directly from the index
                if (otherTableRefs.isEmpty() || isMaintainedOnClient(table)) {
                    mutations.put(rowKeyPtr, new RowMutationState(PRow.DELETE_MARKER, 0, statement.getConnection().getStatementExecutionCounter(), NULL_ROWTIMESTAMP_INFO, null));
                }
                for (int i = 0; i < otherTableRefs.size(); i++) {
                    PTable otherTable = otherTableRefs.get(i).getTable();
                    ImmutableBytesPtr otherRowKeyPtr = new ImmutableBytesPtr(); // allocate new as this is a key in a Map
                    // Translate the data table row to the index table row
                    if (table.getType() == PTableType.INDEX) {
                        otherRowKeyPtr.set(scannedIndexMaintainer.buildDataRowKey(rowKeyPtr, viewConstants));
                        if (otherTable.getType() == PTableType.INDEX) {
                            otherRowKeyPtr.set(maintainers[i].buildRowKey(getter, otherRowKeyPtr, null, null, rs.getCurrentRow().getValue(0).getTimestamp()));
                        }
                    } else {
                        otherRowKeyPtr.set(maintainers[i].buildRowKey(getter, rowKeyPtr, null, null, rs.getCurrentRow().getValue(0).getTimestamp()));
                    }
                    otherMutations.get(i).put(otherRowKeyPtr, new RowMutationState(PRow.DELETE_MARKER, 0, statement.getConnection().getStatementExecutionCounter(), NULL_ROWTIMESTAMP_INFO, null));
                }
                if (mutations.size() > maxSize) {
                    throw new IllegalArgumentException("MutationState size of " + mutations.size() + " is bigger than max allowed size of " + maxSize);
                }
                rowCount++;
                // Commit a batch if we are flushing automatically and we're at our batch size
                if (autoFlush && rowCount % batchSize == 0) {
                    MutationState state = new MutationState(tableRef, mutations, 0, maxSize, maxSizeBytes, connection);
                    connection.getMutationState().join(state);
                    for (int i = 0; i < otherTableRefs.size(); i++) {
                        MutationState indexState = new MutationState(otherTableRefs.get(i), otherMutations.get(i), 0, maxSize, maxSizeBytes, connection);
                        connection.getMutationState().join(indexState);
                    }
                    connection.getMutationState().send();
                    mutations.clear();
                    if (otherMutations != null) {
                        for (MultiRowMutationState multiRowMutationState : otherMutations) {
                            multiRowMutationState.clear();
                        }
                    }
                }
            }

            // If auto flush is true, this last batch will be committed upon return
            int nCommittedRows = autoFlush ? (rowCount / batchSize * batchSize) : 0;

            // tableRef can be index if the index table is selected by the query plan or if we do the DELETE
            // directly on the index table. In other cases it refers to the data table
            MutationState tableState =
                new MutationState(tableRef, mutations, nCommittedRows, maxSize, maxSizeBytes, connection);
            MutationState state;
            if (otherTableRefs.isEmpty()) {
                state = tableState;
            } else {
                state = new MutationState(maxSize, maxSizeBytes, connection);
                // if there are other table references we need to start with an empty mutation state and
                // then join the other states. We only need to count the data table rows that will be deleted.
                // MutationState.join() correctly maintains that accounting and ignores the index table rows.
                // This way we always return the correct number of rows that are deleted.
                state.join(tableState);
            }
            for (int i = 0; i < otherTableRefs.size(); i++) {
                MutationState indexState = new MutationState(otherTableRefs.get(i), otherMutations.get(i), 0, maxSize, maxSizeBytes, connection);
                state.join(indexState);
            }
            return state;
        }
    }
    
    private static class DeletingParallelIteratorFactory extends MutatingParallelIteratorFactory {
        private QueryPlan queryPlan;
        private List<TableRef> otherTableRefs;
        private TableRef projectedTableRef;
        
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
            StatementContext context = new StatementContext(statement, false);
            MutationState state = deleteRows(context, iterator, queryPlan, projectedTableRef, otherTableRefs);
            return state;
        }
        
        public void setQueryPlan(QueryPlan queryPlan) {
            this.queryPlan = queryPlan;
        }
        
        public void setOtherTableRefs(List<TableRef> otherTableRefs) {
            this.otherTableRefs = otherTableRefs;
        }
        
        public void setProjectedTableRef(TableRef projectedTableRef) {
            this.projectedTableRef = projectedTableRef;
        }
    }
    
    private List<PTable> getClientSideMaintainedIndexes(TableRef tableRef) {
        PTable table = tableRef.getTable();
        if (!table.getIndexes().isEmpty()) {
            List<PTable> nonDisabledIndexes = Lists.newArrayListWithExpectedSize(table.getIndexes().size());
            for (PTable index : table.getIndexes()) {
                if (!index.getIndexState().isDisabled() && isMaintainedOnClient(index)) {
                    nonDisabledIndexes.add(index);
                }
            }
            return nonDisabledIndexes;
        }
        return Collections.emptyList();
    }

    /**
     * Implementation of MutationPlan that is selected if
     * 1) the query is strictly point lookup, and
     * 2) the query has no LIMIT clause.
     */
    public class MultiRowDeleteMutationPlan implements MutationPlan {
        private final List<MutationPlan> plans;
        private final MutationPlan firstPlan;
        private final QueryPlan dataPlan;

        public MultiRowDeleteMutationPlan(QueryPlan dataPlan, @NonNull List<MutationPlan> plans) {
            Preconditions.checkArgument(!plans.isEmpty());
            this.plans = plans;
            this.firstPlan = plans.get(0);
            this.dataPlan = dataPlan;
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
            statement.getConnection().getMutationState().join(state);
            for (MutationPlan plan : plans.subList(1, plans.size())) {
                statement.getConnection().getMutationState().join(plan.execute());
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

        @Override
        public Long getEstimatedRowsToScan() throws SQLException {
            Long estRows = null;
            for (MutationPlan plan : plans) {
                /*
                 * If any of the plan doesn't have estimate information available, then we cannot
                 * provide estimate for the overall plan.
                 */
                if (plan.getEstimatedRowsToScan() == null) {
                    return null;
                }
                estRows = add(estRows, plan.getEstimatedRowsToScan());
            }
            return estRows;
        }

        @Override
        public Long getEstimatedBytesToScan() throws SQLException {
            Long estBytes = null;
            for (MutationPlan plan : plans) {
                /*
                 * If any of the plan doesn't have estimate information available, then we cannot
                 * provide estimate for the overall plan.
                 */
                if (plan.getEstimatedBytesToScan() == null) {
                    return null;
                }
                estBytes = add(estBytes, plan.getEstimatedBytesToScan());
            }
            return estBytes;
        }

        @Override
        public Long getEstimateInfoTimestamp() throws SQLException {
            Long estInfoTimestamp = Long.MAX_VALUE;
            for (MutationPlan plan : plans) {
                Long timestamp = plan.getEstimateInfoTimestamp();
                /*
                 * If any of the plan doesn't have estimate information available, then we cannot
                 * provide estimate for the overall plan.
                 */
                if (timestamp == null) {
                    return timestamp;
                }
                estInfoTimestamp = Math.min(estInfoTimestamp, timestamp);
            }
            return estInfoTimestamp;
        }

        @Override
        public QueryPlan getQueryPlan() {
            return dataPlan;
        }
    }

    public MutationPlan compile(DeleteStatement delete) throws SQLException {
        final PhoenixConnection connection = statement.getConnection();
        final boolean isAutoCommit = connection.getAutoCommit();
        final boolean hasPostProcessing = delete.getLimit() != null;
        final ConnectionQueryServices services = connection.getQueryServices();
        List<QueryPlan> queryPlans;
        boolean allowServerMutations =
                services.getProps().getBoolean(QueryServices.ENABLE_SERVER_SIDE_DELETE_MUTATIONS,
                        QueryServicesOptions.DEFAULT_ENABLE_SERVER_SIDE_DELETE_MUTATIONS);
        NamedTableNode tableNode = delete.getTable();
        String tableName = tableNode.getName().getTableName();
        String schemaName = tableNode.getName().getSchemaName();
        SelectStatement select = null;
        ColumnResolver resolverToBe = null;
        DeletingParallelIteratorFactory parallelIteratorFactoryToBe;
        resolverToBe = FromCompiler.getResolverForMutation(delete, connection);
        final TableRef targetTableRef = resolverToBe.getTables().get(0);
        PTable table = targetTableRef.getTable();
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
        
        List<PTable> clientSideIndexes = getClientSideMaintainedIndexes(targetTableRef);
        final boolean hasClientSideIndexes = !clientSideIndexes.isEmpty();

        boolean isSalted = table.getBucketNum() != null;
        boolean isMultiTenant = connection.getTenantId() != null && table.isMultiTenant();
        boolean isSharedViewIndex = table.getViewIndexId() != null;
        int pkColumnOffset = (isSalted ? 1 : 0) + (isMultiTenant ? 1 : 0) + (isSharedViewIndex ? 1 : 0);
        final int pkColumnCount = table.getPKColumns().size() - pkColumnOffset;
        int selectColumnCount = pkColumnCount;
        for (PTable index : clientSideIndexes) {
            selectColumnCount += index.getPKColumns().size() - pkColumnCount;
        }
        Set<PColumn> projectedColumns = new LinkedHashSet<PColumn>(selectColumnCount + pkColumnOffset);
        List<AliasedNode> aliasedNodes = Lists.newArrayListWithExpectedSize(selectColumnCount);
        for (int i = isSalted ? 1 : 0; i < pkColumnOffset; i++) {
            PColumn column = table.getPKColumns().get(i);
            projectedColumns.add(column);
        }
        for (int i = pkColumnOffset; i < table.getPKColumns().size(); i++) {
            PColumn column = table.getPKColumns().get(i);
            projectedColumns.add(column);
            aliasedNodes.add(FACTORY.aliasedNode(null, FACTORY.column(null, '"' + column.getName().getString() + '"', null)));
        }
        // Project all non PK indexed columns so that we can do the proper index maintenance on the indexes for which
        // mutations are generated on the client side. Indexed columns are needed to identify index rows to be deleted
        for (PTable index : table.getIndexes()) {
            if (isMaintainedOnClient(index)) {
                IndexMaintainer maintainer = index.getIndexMaintainer(table, connection);
                // Go through maintainer as it handles functional indexes correctly
                for (Pair<String, String> columnInfo : maintainer.getIndexedColumnInfo()) {
                    String familyName = columnInfo.getFirst();
                    if (familyName != null) {
                        String columnName = columnInfo.getSecond();
                        boolean hasNoColumnFamilies = table.getColumnFamilies().isEmpty();
                        PColumn column = hasNoColumnFamilies ? table.getColumnForColumnName(columnName) : table.getColumnFamily(familyName).getPColumnForColumnName(columnName);
                        if (!projectedColumns.contains(column)) {
                            projectedColumns.add(column);
                            aliasedNodes.add(FACTORY.aliasedNode(null, FACTORY.column(hasNoColumnFamilies ? null : TableName.create(null, familyName), '"' + columnName + '"', null)));
                        }
                    }
                }
            }
        }
        select = FACTORY.select(delete.getTable(), delete.getHint(), false, aliasedNodes, delete.getWhere(),
                Collections.<ParseNode> emptyList(), null, delete.getOrderBy(), delete.getLimit(), null,
                delete.getBindCount(), false, false, Collections.<SelectStatement> emptyList(),
                delete.getUdfParseNodes());
        select = StatementNormalizer.normalize(select, resolverToBe);
        
        SelectStatement transformedSelect = SubqueryRewriter.transform(select, resolverToBe, connection);
        boolean hasPreProcessing = transformedSelect != select;
        if (transformedSelect != select) {
            resolverToBe = FromCompiler.getResolverForQuery(transformedSelect, connection, false, delete.getTable().getName());
            select = StatementNormalizer.normalize(transformedSelect, resolverToBe);
        }
        final boolean hasPreOrPostProcessing = hasPreProcessing || hasPostProcessing;
        boolean noQueryReqd = !hasPreOrPostProcessing;
        // No limit and no sub queries, joins, etc in where clause
        // Can't run on same server for transactional data, as we need the row keys for the data
        // that is being upserted for conflict detection purposes.
        // If we have immutable indexes, we'd increase the number of bytes scanned by executing
        // separate queries against each index, so better to drive from a single table in that case.
        boolean runOnServer = isAutoCommit && !hasPreOrPostProcessing && !table.isTransactional() && !hasClientSideIndexes && allowServerMutations;
        HintNode hint = delete.getHint();
        if (runOnServer && !delete.getHint().hasHint(Hint.USE_INDEX_OVER_DATA_TABLE)) {
            select = SelectStatement.create(select, HintNode.create(hint, Hint.USE_DATA_OVER_INDEX_TABLE));
        }
        
        parallelIteratorFactoryToBe = hasPreOrPostProcessing ? null : new DeletingParallelIteratorFactory(connection);
        QueryOptimizer optimizer = new QueryOptimizer(services);
        QueryCompiler compiler = new QueryCompiler(statement, select, resolverToBe, Collections.<PColumn>emptyList(), parallelIteratorFactoryToBe, new SequenceManager(statement));
        final QueryPlan dataPlan = compiler.compile();
        // TODO: the select clause should know that there's a sub query, but doesn't seem to currently
        queryPlans = Lists.newArrayList(!clientSideIndexes.isEmpty()
                ? optimizer.getApplicablePlans(dataPlan, statement, select, resolverToBe, Collections.<PColumn>emptyList(), parallelIteratorFactoryToBe)
                : optimizer.getBestPlan(dataPlan, statement, select, resolverToBe, Collections.<PColumn>emptyList(), parallelIteratorFactoryToBe));

        runOnServer &= queryPlans.get(0).getTableRef().getTable().getType() != PTableType.INDEX;

        // We need to have all indexed columns available in all immutable indexes in order
        // to generate the delete markers from the query. We also cannot have any filters
        // except for our SkipScanFilter for point lookups.
        // A simple check of the non existence of a where clause in the parse node is not sufficient, as the where clause
        // may have been optimized out. Instead, we check that there's a single SkipScanFilter
        // If we can generate a plan for every index, that means all the required columns are available in every index,
        // hence we can drive the delete from any of the plans.
        noQueryReqd &= queryPlans.size() == 1 + clientSideIndexes.size();
        int queryPlanIndex = 0;
        while (noQueryReqd && queryPlanIndex < queryPlans.size()) {
            QueryPlan plan = queryPlans.get(queryPlanIndex++);
            StatementContext context = plan.getContext();
            noQueryReqd &= (!context.getScan().hasFilter()
                    || context.getScan().getFilter() instanceof SkipScanFilter)
                && context.getScanRanges().isPointLookup();
        }

        final int maxSize = services.getProps().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB,QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
        final long maxSizeBytes = services.getProps()
                .getLongBytes(QueryServices.MAX_MUTATION_SIZE_BYTES_ATTRIB,
                        QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE_BYTES);
 
        // If we're doing a query for a set of rows with no where clause, then we don't need to contact the server at all.
        if (noQueryReqd) {
            // Create a mutationPlan for each queryPlan. One plan will be for the deletion of the rows
            // from the data table, while the others will be for deleting rows from immutable indexes.
            List<MutationPlan> mutationPlans = Lists.newArrayListWithExpectedSize(queryPlans.size());
            for (final QueryPlan plan : queryPlans) {
                mutationPlans.add(new SingleRowDeleteMutationPlan(plan, connection, maxSize, maxSizeBytes));
            }
            return new MultiRowDeleteMutationPlan(dataPlan, mutationPlans);
        } else if (runOnServer) {
            // TODO: better abstraction
            final StatementContext context = dataPlan.getContext();
            Scan scan = context.getScan();
            scan.setAttribute(BaseScannerRegionObserverConstants.DELETE_AGG, QueryConstants.TRUE);

            // Build an ungrouped aggregate query: select COUNT(*) from <table> where <where>
            // The coprocessor will delete each row returned from the scan
            // Ignoring ORDER BY, since with auto commit on and no limit makes no difference
            SelectStatement aggSelect = SelectStatement.create(SelectStatement.COUNT_ONE, delete.getHint());
            RowProjector projectorToBe = ProjectionCompiler.compile(context, aggSelect, GroupBy.EMPTY_GROUP_BY);
            context.getAggregationManager().compile(context, GroupBy.EMPTY_GROUP_BY);
            if (dataPlan.getProjector().projectEveryRow()) {
                projectorToBe = new RowProjector(projectorToBe,true);
            }
            final RowProjector projector = projectorToBe;
            final QueryPlan aggPlan = new AggregatePlan(context, select, dataPlan.getTableRef(), projector, null, null,
                    OrderBy.EMPTY_ORDER_BY, null, GroupBy.EMPTY_GROUP_BY, null, dataPlan);
            return new ServerSelectDeleteMutationPlan(dataPlan, connection, aggPlan, projector, maxSize, maxSizeBytes);
        } else {
            final DeletingParallelIteratorFactory parallelIteratorFactory = parallelIteratorFactoryToBe;
            List<PColumn> adjustedProjectedColumns = Lists.newArrayListWithExpectedSize(projectedColumns.size());
            final int offset = table.getBucketNum() == null ? 0 : 1;
            Iterator<PColumn> projectedColsItr = projectedColumns.iterator();
            int i = 0;
            while (projectedColsItr.hasNext()) {
                final int position = i++;
                adjustedProjectedColumns.add(new DelegateColumn(projectedColsItr.next()) {
                    @Override
                    public int getPosition() {
                        return position + offset;
                    }
                });
            }
            PTable projectedTable = PTableImpl.builderWithColumns(table, adjustedProjectedColumns)
                    .setType(PTableType.PROJECTED)
                    .build();
            final TableRef projectedTableRef = new TableRef(projectedTable, targetTableRef.getLowerBoundTimeStamp(), targetTableRef.getTimeStamp());

            QueryPlan bestPlanToBe = dataPlan;
            for (QueryPlan plan : queryPlans) {
                PTable planTable = plan.getTableRef().getTable();
                if (planTable.getIndexState() != PIndexState.BUILDING) {
                    bestPlanToBe = plan;
                    break;
                }
            }
            final QueryPlan bestPlan = bestPlanToBe;
            final List<TableRef>otherTableRefs = Lists.newArrayListWithExpectedSize(clientSideIndexes.size());
            for (PTable index : clientSideIndexes) {
                if (!bestPlan.getTableRef().getTable().equals(index)) {
                    otherTableRefs.add(new TableRef(index, targetTableRef.getLowerBoundTimeStamp(), targetTableRef.getTimeStamp()));
                }
            }
            
            if (!bestPlan.getTableRef().getTable().equals(targetTableRef.getTable())) {
                otherTableRefs.add(projectedTableRef);
            }
            return new ClientSelectDeleteMutationPlan(targetTableRef, dataPlan, bestPlan, hasPreOrPostProcessing,
                    parallelIteratorFactory, otherTableRefs, projectedTableRef, maxSize, maxSizeBytes, connection);
        }
    }

    /**
     * Implementation of MutationPlan for composing a MultiRowDeleteMutationPlan.
     */
    private class SingleRowDeleteMutationPlan implements MutationPlan {

        private final QueryPlan dataPlan;
        private final PhoenixConnection connection;
        private final int maxSize;
        private final StatementContext context;
        private final long maxSizeBytes;

        public SingleRowDeleteMutationPlan(QueryPlan dataPlan, PhoenixConnection connection, int maxSize, long maxSizeBytes) {
            this.dataPlan = dataPlan;
            this.connection = connection;
            this.maxSize = maxSize;
            this.context = dataPlan.getContext();
            this.maxSizeBytes = maxSizeBytes;
        }

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
            MultiRowMutationState mutation = new MultiRowMutationState(ranges.getPointLookupCount());
            while (iterator.hasNext()) {
                mutation.put(new ImmutableBytesPtr(iterator.next().getLowerRange()),
                        new RowMutationState(PRow.DELETE_MARKER, 0,
                                statement.getConnection().getStatementExecutionCounter(), NULL_ROWTIMESTAMP_INFO, null));
            }
            return new MutationState(dataPlan.getTableRef(), mutation, 0, maxSize, maxSizeBytes, connection);
        }

        @Override
        public ExplainPlan getExplainPlan() throws SQLException {
            return new ExplainPlan(Collections.singletonList("DELETE SINGLE ROW"));
        }

        @Override
        public QueryPlan getQueryPlan() {
            return dataPlan;
        }

        @Override
        public StatementContext getContext() {
            return context;
        }

        @Override
        public TableRef getTargetRef() {
            return dataPlan.getTableRef();
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

    /**
     * Implementation of MutationPlan that is selected if
     * 1) there is no immutable index presented for the table,
     * 2) auto commit is enabled as well as server side delete mutations are enabled,
     * 3) the table is not transactional,
     * 4) the query has no LIMIT clause, and
     * 5) the query has WHERE clause and is not strictly point lookup.
     */
    public class ServerSelectDeleteMutationPlan implements MutationPlan {
        private final StatementContext context;
        private final QueryPlan dataPlan;
        private final PhoenixConnection connection;
        private final QueryPlan aggPlan;
        private final RowProjector projector;
        private final int maxSize;
        private final long maxSizeBytes;

        public ServerSelectDeleteMutationPlan(QueryPlan dataPlan, PhoenixConnection connection, QueryPlan aggPlan,
                                              RowProjector projector, int maxSize, long maxSizeBytes) {
            this.context = dataPlan.getContext();
            this.dataPlan = dataPlan;
            this.connection = connection;
            this.aggPlan = aggPlan;
            this.projector = projector;
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
            return dataPlan.getTableRef();
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
            PTable table = dataPlan.getTableRef().getTable();
            table.getIndexMaintainers(ptr, context.getConnection());
            ScanUtil.annotateScanWithMetadataAttributes(table, context.getScan());
            byte[] txState = table.isTransactional() ? connection.getMutationState().encodeTransaction() : ByteUtil.EMPTY_BYTE_ARRAY;
            ServerCache cache = null;
            try {
                if (ptr.getLength() > 0) {
                    byte[] uuidValue = ServerCacheClient.generateId();
                    context.getScan().setAttribute(PhoenixIndexCodec.INDEX_UUID, uuidValue);
                    context.getScan().setAttribute(PhoenixIndexCodec.INDEX_PROTO_MD, ptr.get());
                    context.getScan().setAttribute(BaseScannerRegionObserverConstants.TX_STATE, txState);
                    ScanUtil.setClientVersion(context.getScan(), MetaDataProtocol.PHOENIX_VERSION);
                    String sourceOfDelete = statement.getConnection().getSourceOfOperation();
                    if (sourceOfDelete != null) {
                        context.getScan().setAttribute(QueryServices.SOURCE_OPERATION_ATTRIB,
                                Bytes.toBytes(sourceOfDelete));
                    }
                }
                ResultIterator iterator = aggPlan.iterator();
                try {
                    Tuple row = iterator.next();
                    final long mutationCount = (Long) projector.getColumnProjector(0).getValue(row, PLong.INSTANCE, ptr);
                    return new MutationState(maxSize, maxSizeBytes, connection) {
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
            ExplainPlan explainPlan = aggPlan.getExplainPlan();
            List<String> queryPlanSteps = explainPlan.getPlanSteps();
            ExplainPlanAttributes explainPlanAttributes =
                explainPlan.getPlanStepsAsAttributes();
            List<String> planSteps =
                Lists.newArrayListWithExpectedSize(queryPlanSteps.size() + 1);
            ExplainPlanAttributesBuilder newBuilder =
                new ExplainPlanAttributesBuilder(explainPlanAttributes);
            newBuilder.setAbstractExplainPlan("DELETE ROWS SERVER SELECT");
            planSteps.add("DELETE ROWS SERVER SELECT");
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

        @Override
        public QueryPlan getQueryPlan() {
            return aggPlan;
        }
    }

    /**
     * Implementation of MutationPlan that is selected if the query doesn't match the criteria of
     * ServerSelectDeleteMutationPlan.
     */
    public class ClientSelectDeleteMutationPlan implements MutationPlan {
        private final StatementContext context;
        private final TableRef targetTableRef;
        private final QueryPlan dataPlan;
        private final QueryPlan bestPlan;
        private final boolean hasPreOrPostProcessing;
        private final DeletingParallelIteratorFactory parallelIteratorFactory;
        private final List<TableRef> otherTableRefs;
        private final TableRef projectedTableRef;
        private final int maxSize;
        private final long maxSizeBytes;
        private final PhoenixConnection connection;

        public ClientSelectDeleteMutationPlan(TableRef targetTableRef, QueryPlan dataPlan, QueryPlan bestPlan,
                                              boolean hasPreOrPostProcessing,
                                              DeletingParallelIteratorFactory parallelIteratorFactory,
                                              List<TableRef> otherTableRefs, TableRef projectedTableRef, int maxSize,
                                              long maxSizeBytes, PhoenixConnection connection) {
            this.context = bestPlan.getContext();
            this.targetTableRef = targetTableRef;
            this.dataPlan = dataPlan;
            this.bestPlan = bestPlan;
            this.hasPreOrPostProcessing = hasPreOrPostProcessing;
            this.parallelIteratorFactory = parallelIteratorFactory;
            this.otherTableRefs = otherTableRefs;
            this.projectedTableRef = projectedTableRef;
            this.maxSize = maxSize;
            this.maxSizeBytes = maxSizeBytes;
            this.connection = connection;
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
            return targetTableRef;
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
            ResultIterator iterator = bestPlan.iterator();
            try {
                // If we're not doing any pre or post processing, we can produce the delete mutations directly
                // in the parallel threads executed for the scan
                if (!hasPreOrPostProcessing) {
                    Tuple tuple;
                    long totalRowCount = 0;
                    if (parallelIteratorFactory != null) {
                        parallelIteratorFactory.setQueryPlan(bestPlan);
                        parallelIteratorFactory.setOtherTableRefs(otherTableRefs);
                        parallelIteratorFactory.setProjectedTableRef(projectedTableRef);
                    }
                    while ((tuple=iterator.next()) != null) {// Runs query
                        Cell kv = tuple.getValue(0);
                        totalRowCount += PLong.INSTANCE.getCodec().decodeLong(kv.getValueArray(), kv.getValueOffset(), SortOrder.getDefault());
                    }
                    // Return total number of rows that have been deleted from the table. In the case of auto commit being off
                    // the mutations will all be in the mutation state of the current connection.
                    MutationState state = new MutationState(maxSize, maxSizeBytes, connection, totalRowCount);

                    // set the read metrics accumulated in the parent context so that it can be published when the mutations are committed.
                    state.setReadMetricQueue(context.getReadMetricsQueue());

                    return state;
                } else {
                    // Otherwise, we have to execute the query and produce the delete mutations in the single thread
                    // producing the query results.
                    return deleteRows(context, iterator, bestPlan, projectedTableRef, otherTableRefs);
                }
            } finally {
                iterator.close();
            }
        }

        @Override
        public ExplainPlan getExplainPlan() throws SQLException {
            ExplainPlan explainPlan = bestPlan.getExplainPlan();
            List<String> queryPlanSteps = explainPlan.getPlanSteps();
            ExplainPlanAttributes explainPlanAttributes =
                explainPlan.getPlanStepsAsAttributes();
            List<String> planSteps = Lists.newArrayListWithExpectedSize(queryPlanSteps.size()+1);
            ExplainPlanAttributesBuilder newBuilder =
                new ExplainPlanAttributesBuilder(explainPlanAttributes);
            newBuilder.setAbstractExplainPlan("DELETE ROWS CLIENT SELECT");
            planSteps.add("DELETE ROWS CLIENT SELECT");
            planSteps.addAll(queryPlanSteps);
            return new ExplainPlan(planSteps, newBuilder.build());
        }

        @Override
        public Long getEstimatedRowsToScan() throws SQLException {
            return bestPlan.getEstimatedRowsToScan();
        }

        @Override
        public Long getEstimatedBytesToScan() throws SQLException {
            return bestPlan.getEstimatedBytesToScan();
        }

        @Override
        public Long getEstimateInfoTimestamp() throws SQLException {
            return bestPlan.getEstimateInfoTimestamp();
        }

        @Override
        public QueryPlan getQueryPlan() {
            return bestPlan;
        }
    }
    
    private static boolean isMaintainedOnClient(PTable table) {
        // Test for not being local (rather than being GLOBAL) so that this doesn't fail
        // when tested with our projected table.
        return (table.getIndexType() != IndexType.LOCAL && (table.isTransactional() || table.isImmutableRows())) ||
               (table.getIndexType() == IndexType.LOCAL && (table.isTransactional() &&
                table.getTransactionProvider().getTransactionProvider().isUnsupported(Feature.MAINTAIN_LOCAL_INDEX_ON_SERVER) ) );
    }
    
}
