package org.apache.phoenix.calcite.rel;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.hadoop.hbase.Cell;
import org.apache.phoenix.calcite.PhoenixTable;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.StatementPlan;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PLong;

import com.google.common.collect.Lists;

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
        final TableRef targetTableRef = getTable().unwrap(PhoenixTable.class).tableMapping.getTableRef();
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
//                ResultIterator iterator = queryPlan.iterator();
//                if (parallelIteratorFactory == null) {
//                    return upsertSelect(new StatementContext(statement), tableRef, projector, iterator, columnIndexes, pkSlotIndexes, useServerTimestamp);
//                }
//                try {
//                    parallelIteratorFactory.setRowProjector(projector);
//                    parallelIteratorFactory.setColumnIndexes(columnIndexes);
//                    parallelIteratorFactory.setPkSlotIndexes(pkSlotIndexes);
//                    Tuple tuple;
//                    long totalRowCount = 0;
//                    StatementContext context = queryPlan.getContext();
//                    while ((tuple=iterator.next()) != null) {// Runs query
//                        Cell kv = tuple.getValue(0);
//                        totalRowCount += PLong.INSTANCE.getCodec().decodeLong(kv.getValueArray(), kv.getValueOffset(), SortOrder.getDefault());
//                    }
//                    // Return total number of rows that have been updated. In the case of auto commit being off
//                    // the mutations will all be in the mutation state of the current connection.
//                    MutationState mutationState = new MutationState(maxSize, statement.getConnection(), totalRowCount);
//                    /*
//                     *  All the metrics collected for measuring the reads done by the parallel mutating iterators
//                     *  is included in the ReadMetricHolder of the statement context. Include these metrics in the
//                     *  returned mutation state so they can be published on commit. 
//                     */
//                    mutationState.setReadMetricQueue(context.getReadMetricsQueue());
//                    return mutationState; 
//                } finally {
//                    iterator.close();
//                }
                return null;
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

}
