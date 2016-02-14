package org.apache.phoenix.calcite.rel;

import java.sql.SQLException;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.phoenix.calcite.CalciteUtils;
import org.apache.phoenix.calcite.metadata.PhoenixRelMdCollation;
import org.apache.phoenix.compile.JoinCompiler;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.execute.CorrelatePlan;
import org.apache.phoenix.parse.JoinTableNode.JoinType;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;

import com.google.common.base.Supplier;

public class PhoenixCorrelate extends Correlate implements PhoenixRel {
    
    public static PhoenixCorrelate create(final RelNode left, final RelNode right, 
            CorrelationId correlationId, ImmutableBitSet requiredColumns, 
            final SemiJoinType joinType) {
        final RelOptCluster cluster = left.getCluster();
        final RelMetadataQuery mq = RelMetadataQuery.instance();
        final RelTraitSet traits =
                cluster.traitSet().replace(PhoenixConvention.CLIENT)
                .replaceIfs(RelCollationTraitDef.INSTANCE,
                        new Supplier<List<RelCollation>>() {
                    public List<RelCollation> get() {
                        return PhoenixRelMdCollation.correlate(mq, left, right, joinType);
                    }
                });
        return new PhoenixCorrelate(cluster, traits, left, right, correlationId,
                requiredColumns, joinType);
    }

    private PhoenixCorrelate(RelOptCluster cluster, RelTraitSet traits,
            RelNode left, RelNode right, CorrelationId correlationId,
            ImmutableBitSet requiredColumns, SemiJoinType joinType) {
        super(cluster, traits, left, right, correlationId, requiredColumns,
                joinType);
    }

    @Override
    public Correlate copy(RelTraitSet traitSet, RelNode left, RelNode right,
            CorrelationId correlationId, ImmutableBitSet requiredColumns,
            SemiJoinType joinType) {
        return create(left, right, correlationId, requiredColumns, joinType);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (!getLeft().getConvention().satisfies(PhoenixConvention.GENERIC)
                || !getRight().getConvention().satisfies(PhoenixConvention.GENERIC))
            return planner.getCostFactory().makeInfiniteCost();

        double rowCount = mq.getRowCount(this);

        final double rightRowCount = right.estimateRowCount(mq);
        final double leftRowCount = left.estimateRowCount(mq);
        if (Double.isInfinite(leftRowCount) || Double.isInfinite(rightRowCount)) {
            return planner.getCostFactory().makeInfiniteCost();
        }

        Double restartCount = mq.getRowCount(getLeft());
        // RelMetadataQuery.getCumulativeCost(getRight()); does not work for
        // RelSubset, so we ask planner to cost-estimate right relation
        RelOptCost rightCost = planner.getCost(getRight(), mq);
        RelOptCost rescanCost =
                rightCost.multiplyBy(Math.max(1.0, restartCount - 1));

        return planner.getCostFactory().makeCost(0,
                rowCount /* generate results */ + leftRowCount /* scan left results */,
                0).plus(rescanCost);
    }
    
    @Override
    public QueryPlan implement(Implementor implementor) {
        implementor.pushContext(new ImplementorContext(implementor.getCurrentContext().retainPKColumns, true, ImmutableIntList.identity(getLeft().getRowType().getFieldCount())));
        QueryPlan leftPlan = implementor.visitInput(0, (PhoenixRel) getLeft());
        PTable leftTable = implementor.getTableRef().getTable();
        implementor.popContext();

        implementor.getRuntimeContext().defineCorrelateVariable(getCorrelVariable(), implementor.getTableRef());

        implementor.pushContext(new ImplementorContext(false, true, ImmutableIntList.identity(getRight().getRowType().getFieldCount())));
        QueryPlan rightPlan = implementor.visitInput(1, (PhoenixRel) getRight());
        PTable rightTable = implementor.getTableRef().getTable();
        implementor.popContext();
                
        JoinType type = CalciteUtils.convertSemiJoinType(getJoinType());
        PTable joinedTable;
        try {
            joinedTable = JoinCompiler.joinProjectedTables(leftTable, rightTable, type);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        TableRef tableRef = new TableRef(joinedTable);
        implementor.setTableRef(tableRef);

        return new CorrelatePlan(leftPlan, rightPlan, getCorrelVariable(), 
                type, false, implementor.getRuntimeContext(), joinedTable, 
                leftTable, rightTable, leftTable.getColumns().size() - leftTable.getPKColumns().size());
    }

}
