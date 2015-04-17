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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.execute.HashJoinPlan;
import org.apache.phoenix.execute.ScanPlan;
import org.apache.phoenix.execute.TupleProjector;

import com.google.common.base.Supplier;

public class PhoenixServerProject extends PhoenixAbstractProject {
    
    public static PhoenixServerProject create(final RelNode input, 
            final List<? extends RexNode> projects, RelDataType rowType) {
        RelOptCluster cluster = input.getCluster();
        final RelTraitSet traits =
                cluster.traitSet().replace(PhoenixRel.CONVENTION)
                .replaceIfs(RelCollationTraitDef.INSTANCE,
                        new Supplier<List<RelCollation>>() {
                    public List<RelCollation> get() {
                        return RelMdCollation.project(input, projects);
                    }
                });
        return new PhoenixServerProject(cluster, traits, input, projects, rowType);
    }

    private PhoenixServerProject(RelOptCluster cluster, RelTraitSet traits,
            RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
        super(cluster, traits, input, projects, rowType);
    }

    @Override
    public PhoenixServerProject copy(RelTraitSet traits, RelNode input,
            List<RexNode> projects, RelDataType rowType) {
        return new PhoenixServerProject(getCluster(), traits, input, projects, rowType);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        return super.computeSelfCost(planner)
                .multiplyBy(SERVER_FACTOR)
                .multiplyBy(PHOENIX_FACTOR);
    }

    @Override
    public QueryPlan implement(Implementor implementor) {
        assert getConvention() == getInput().getConvention();
        
        implementor.pushContext(new ImplementorContext(implementor.getCurrentContext().isRetainPKColumns(), false));
        QueryPlan plan = implementor.visitInput(0, (PhoenixRel) getInput());
        implementor.popContext();
        assert (plan instanceof ScanPlan || plan instanceof HashJoinPlan) 
                && !TupleProjector.hasProjector(plan.getContext().getScan(), plan instanceof ScanPlan);
        
        TupleProjector tupleProjector = super.project(implementor);
        TupleProjector.serializeProjectorIntoScan(plan.getContext().getScan(), tupleProjector, plan instanceof ScanPlan);
        return plan;
    }
}
