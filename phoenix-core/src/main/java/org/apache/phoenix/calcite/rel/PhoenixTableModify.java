package org.apache.phoenix.calcite.rel;

import static org.apache.phoenix.execute.MutationState.RowTimestampColInfo.NULL_ROWTIMESTAMP_INFO;

import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.calcite.PhoenixTable;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.StatementPlan;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.execute.MutationState.RowMutationState;
import org.apache.phoenix.execute.MutationState.RowTimestampColInfo;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PRow;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class PhoenixTableModify extends TableModify implements PhoenixRel {

    public PhoenixTableModify(RelOptCluster cluster, RelTraitSet traits,
            RelOptTable table, CatalogReader catalogReader, RelNode child,
            Operation operation, List<String> updateColumnList, boolean flattened) {
        super(cluster, traits, table, catalogReader, child, operation, updateColumnList, flattened);
        assert operation == Operation.INSERT || operation == Operation.DELETE;
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new PhoenixTableModify(
          getCluster(),
          traitSet,
          getTable(),
          getCatalogReader(),
          sole(inputs),
          getOperation(),
          getUpdateColumnList(),
          isFlattened());
    }

    @Override
    public StatementPlan implement(PhoenixRelImplementor implementor) {
        final QueryPlan queryPlan = implementor.visitInput(0, (PhoenixQueryRel) input);
        final RowProjector projector = implementor.getTableMapping().createRowProjector();

        final PhoenixTable targetTable = getTable().unwrap(PhoenixTable.class);
        final PhoenixConnection connection = targetTable.pc;
        final TableRef targetTableRef = targetTable.tableMapping.getTableRef();
        
        if (getOperation() == Operation.INSERT) {
            return upsert(connection, targetTable, targetTableRef, queryPlan, projector);
        }
        
        // delete
        return delete(connection, targetTable, targetTableRef, queryPlan, projector);
    }
    
    private static MutationPlan upsert(final PhoenixConnection connection,
            final PhoenixTable targetTable, final TableRef targetTableRef,
            final QueryPlan queryPlan, final RowProjector projector) {
        try (PhoenixStatement stmt = new PhoenixStatement(connection)) {
            final ColumnResolver resolver = FromCompiler.getResolver(targetTableRef);
            final StatementContext context = new StatementContext(stmt, resolver, new Scan(), new SequenceManager(stmt));

            // TODO TenantId, ViewIndexId, UpdatableViewColumns
            final List<PColumn> mappedColumns = targetTable.tableMapping.getMappedColumns();
            final int[] columnIndexes = new int[mappedColumns.size()];
            final int[] pkSlotIndexes = new int[mappedColumns.size()];
            for (int i = 0; i < columnIndexes.length; i++) {
                PColumn column = mappedColumns.get(i);
                if (SchemaUtil.isPKColumn(column)) {
                    pkSlotIndexes[i] = column.getPosition();
                }
                columnIndexes[i] = column.getPosition();
            }
            // TODO
            final boolean useServerTimestamp = false;
            
            return new MutationPlan() {
                @Override
                public ParameterMetaData getParameterMetaData() {
                    return queryPlan.getContext().getBindManager().getParameterMetaData();
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
                    // TODO return originalQueryPlan.getSourceRefs();
                    return queryPlan.getSourceRefs();
                }

                @Override
                public org.apache.phoenix.jdbc.PhoenixStatement.Operation getOperation() {
                    return org.apache.phoenix.jdbc.PhoenixStatement.Operation.UPSERT;
                }

                @Override
                public MutationState execute() throws SQLException {
                    ResultIterator iterator = queryPlan.iterator();
                    // simplest version, no run-on-server, no pipelined update
                    StatementContext childContext = queryPlan.getContext();
                    ConnectionQueryServices services = connection.getQueryServices();
                    int maxSize = services.getProps().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB,
                            QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
                    int batchSize = Math.min(connection.getMutateBatchSize(), maxSize);
                    boolean isAutoCommit = connection.getAutoCommit();
                    byte[][] values = new byte[columnIndexes.length][];
                    int rowCount = 0;
                    Map<ImmutableBytesPtr, RowMutationState> mutation = Maps.newHashMapWithExpectedSize(batchSize);
                    PTable table = targetTableRef.getTable();
                    try (ResultSet rs = new PhoenixResultSet(iterator, projector, childContext)) {
                        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                        while (rs.next()) {
                            for (int i = 0; i < values.length; i++) {
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
                                if (!column.getDataType().isSizeCompatible(ptr, value, column.getDataType(), SortOrder.getDefault(), precision,
                                        scale, column.getMaxLength(), column.getScale())) { throw new SQLExceptionInfo.Builder(
                                        SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY).setColumnName(column.getName().getString())
                                        .setMessage("value=" + column.getDataType().toStringLiteral(ptr, null)).build()
                                        .buildException(); }
                                column.getDataType().coerceBytes(ptr, value, column.getDataType(), 
                                        precision, scale, SortOrder.getDefault(), 
                                        column.getMaxLength(), column.getScale(), column.getSortOrder(),
                                        table.rowKeyOrderOptimizable());
                                values[i] = ByteUtil.copyKeyBytesIfNecessary(ptr);
                            }
                            // TODO onDupKeyBytes
                            setValues(values, pkSlotIndexes, columnIndexes, table, mutation, connection, useServerTimestamp, null);
                            rowCount++;
                            // Commit a batch if auto commit is true and we're at our batch size
                            if (isAutoCommit && rowCount % batchSize == 0) {
                                MutationState state = new MutationState(targetTableRef, mutation, 0, maxSize, connection);
                                connection.getMutationState().join(state);
                                connection.getMutationState().send();
                                mutation.clear();
                            }
                        }
                        // If auto commit is true, this last batch will be committed upon return
                        return new MutationState(targetTableRef, mutation, rowCount / batchSize * batchSize, maxSize, connection);
                    }
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
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    private static void setValues(byte[][] values, int[] pkSlotIndex, int[] columnIndexes, PTable table, Map<ImmutableBytesPtr,RowMutationState> mutation, PhoenixConnection connection, boolean useServerTimestamp, byte[] onDupKeyBytes) {
        Map<PColumn,byte[]> columnValues = Maps.newHashMapWithExpectedSize(columnIndexes.length);
        byte[][] pkValues = new byte[table.getPKColumns().size()][];
        // If the table uses salting, the first byte is the salting byte, set to an empty array
        // here and we will fill in the byte later in PRowImpl.
        if (table.getBucketNum() != null) {
            pkValues[0] = new byte[] {0};
        }
        Long rowTimestamp = null; // case when the table doesn't have a row timestamp column
        RowTimestampColInfo rowTsColInfo = new RowTimestampColInfo(useServerTimestamp, rowTimestamp);
        for (int i = 0; i < values.length; i++) {
            byte[] value = values[i];
            PColumn column = table.getColumns().get(columnIndexes[i]);
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
            }
        }
        ImmutableBytesPtr ptr = new ImmutableBytesPtr();
        table.newKey(ptr, pkValues);
        mutation.put(ptr, new RowMutationState(columnValues, connection.getStatementExecutionCounter(), rowTsColInfo, onDupKeyBytes));
    }

    private static MutationPlan delete(final PhoenixConnection connection,
            final PhoenixTable targetTable, final TableRef targetTableRef,
            final QueryPlan queryPlan, final RowProjector projector) {
        final StatementContext context = queryPlan.getContext();
        // TODO
        final boolean deleteFromImmutableIndexToo = false;
        return new MutationPlan() {
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
                // TODO dataPlan.getSourceRefs();
                return queryPlan.getSourceRefs();
            }

            @Override
            public org.apache.phoenix.jdbc.PhoenixStatement.Operation getOperation() {
                return org.apache.phoenix.jdbc.PhoenixStatement.Operation.DELETE;
            }

            @Override
            public MutationState execute() throws SQLException {
                ResultIterator iterator = queryPlan.iterator();
                try {
                    // TODO hasLimit??
                    return deleteRows(context, targetTableRef, deleteFromImmutableIndexToo ? queryPlan.getTableRef() : null, iterator, projector, queryPlan.getTableRef());
                } finally {
                    iterator.close();
                }
            }

            @Override
            public ExplainPlan getExplainPlan() throws SQLException {
                List<String> queryPlanSteps =  queryPlan.getExplainPlan().getPlanSteps();
                List<String> planSteps = Lists.newArrayListWithExpectedSize(queryPlanSteps.size()+1);
                planSteps.add("DELETE ROWS");
                planSteps.addAll(queryPlanSteps);
                return new ExplainPlan(planSteps);
            }
        };
    }
    
    private static MutationState deleteRows(StatementContext childContext, TableRef targetTableRef, TableRef indexTableRef, ResultIterator iterator, RowProjector projector, TableRef sourceTableRef) throws SQLException {
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
        Map<ImmutableBytesPtr,RowMutationState> indexMutations = null;
        // If indexTableRef is set, we're deleting the rows from both the index table and
        // the data table through a single query to save executing an additional one.
        if (indexTableRef != null) {
            indexMutations = Maps.newHashMapWithExpectedSize(batchSize);
        }
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
                if (indexTableRef != null) {
                    ImmutableBytesPtr indexPtr = new ImmutableBytesPtr(); // allocate new as this is a key in a Map
                    rs.getCurrentRow().getKey(indexPtr);
                    indexMutations.put(indexPtr, new RowMutationState(PRow.DELETE_MARKER, statement.getConnection().getStatementExecutionCounter(), NULL_ROWTIMESTAMP_INFO, null));
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
                    connection.getMutationState().send();
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
        }
    }
}
