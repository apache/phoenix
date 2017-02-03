package org.apache.phoenix.calcite.rel;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.execute.ClientScanPlan;

import com.google.common.base.Supplier;

public class PhoenixLimit extends Limit implements PhoenixQueryRel {
    
    public static PhoenixLimit create(final RelNode input, RexNode offset, RexNode fetch) {
        final RelOptCluster cluster = input.getCluster();
        final RelMetadataQuery mq = RelMetadataQuery.instance();
        final RelTraitSet traits =
                cluster.traitSet().replace(PhoenixConvention.CLIENT)
                .replaceIfs(RelCollationTraitDef.INSTANCE,
                        new Supplier<List<RelCollation>>() {
                    public List<RelCollation> get() {
                        return RelMdCollation.limit(mq, input);
                    }
                });
        return new PhoenixLimit(cluster, traits, input, offset, fetch);
    }

    private PhoenixLimit(RelOptCluster cluster, RelTraitSet traits, RelNode input, RexNode offset, RexNode fetch) {
        super(cluster, traits, input, offset, fetch);
    }
    
    @Override
    public PhoenixLimit copy(
            RelTraitSet traitSet,
            List<RelNode> newInputs) {
        return create(
                sole(newInputs),
                offset,
                fetch);
    }

    @Override 
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (!getInput().getConvention().satisfies(PhoenixConvention.GENERIC))
            return planner.getCostFactory().makeInfiniteCost();
        double rowCount = mq.getRowCount(this);
        return planner.getCostFactory()
                .makeCost(rowCount, 0, 0)
                .multiplyBy(PHOENIX_FACTOR);
    }

    @Override
    public QueryPlan implement(PhoenixRelImplementor implementor) {
        QueryPlan plan = implementor.visitInput(0, (PhoenixQueryRel) getInput());
        Integer fetch = this.fetch == null ? null : RexLiteral.intValue(this.fetch);
        Integer offset = this.offset == null ? null : RexLiteral.intValue(this.offset);

        if (plan.getLimit() == null && plan.getOffset() == null) {
            return plan.limit(fetch, offset);
        }
        
        return new ClientScanPlan(plan.getContext(), plan.getStatement(), 
                implementor.getTableMapping().getTableRef(), RowProjector.EMPTY_PROJECTOR, 
                fetch, offset, null, OrderBy.EMPTY_ORDER_BY, plan);
    }
}
