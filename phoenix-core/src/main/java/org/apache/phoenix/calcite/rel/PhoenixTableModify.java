package org.apache.phoenix.calcite.rel;

import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.calcite.PhoenixTable;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
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
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class PhoenixTableModify extends TableModify implements PhoenixRel {

    public PhoenixTableModify(RelOptCluster cluster, RelTraitSet traits,
            RelOptTable table, CatalogReader catalogReader, RelNode child,
            Operation operation, List<String> updateColumnList, boolean flattened) {
        super(cluster, traits, table, catalogReader, child, operation, updateColumnList, flattened);
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
        if (getOperation() != Operation.INSERT) {
            throw new UnsupportedOperationException();
        }
        
        final QueryPlan queryPlan = implementor.visitInput(0, (PhoenixQueryRel) input);
        final RowProjector projector = implementor.getTableMapping().createRowProjector();
        final PhoenixTable targetTable = getTable().unwrap(PhoenixTable.class);
        final PhoenixConnection connection = targetTable.pc;
        final TableRef targetTableRef = targetTable.tableMapping.getTableRef();
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
                return queryPlan.getContext();
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
                            if (!column.getDataType().isSizeCompatible(ptr, value, column.getDataType(), precision, scale,
                                    column.getMaxLength(), column.getScale())) { throw new SQLExceptionInfo.Builder(
                                    SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY).setColumnName(column.getName().getString())
                                    .setMessage("value=" + column.getDataType().toStringLiteral(ptr, null)).build()
                                    .buildException(); }
                            column.getDataType().coerceBytes(ptr, value, column.getDataType(), 
                                    precision, scale, SortOrder.getDefault(), 
                                    column.getMaxLength(), column.getScale(), column.getSortOrder(),
                                    table.rowKeyOrderOptimizable());
                            values[i] = ByteUtil.copyKeyBytesIfNecessary(ptr);
                        }
                        setValues(values, pkSlotIndexes, columnIndexes, table, mutation, connection, useServerTimestamp);
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
    }
    
    private static void setValues(byte[][] values, int[] pkSlotIndex, int[] columnIndexes, PTable table, Map<ImmutableBytesPtr,RowMutationState> mutation, PhoenixConnection connection, boolean useServerTimestamp) {
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
        mutation.put(ptr, new RowMutationState(columnValues, connection.getStatementExecutionCounter(), rowTsColInfo));
    }

}
