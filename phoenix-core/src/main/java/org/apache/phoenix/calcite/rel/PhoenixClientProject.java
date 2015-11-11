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
import org.apache.phoenix.execute.TupleProjectionPlan;
import org.apache.phoenix.execute.TupleProjector;

import com.google.common.base.Supplier;

public class PhoenixClientProject extends PhoenixAbstractProject {
    
    public static PhoenixClientProject create(final RelNode input, 
            final List<? extends RexNode> projects, RelDataType rowType) {
        RelOptCluster cluster = input.getCluster();
        final RelTraitSet traits =
                cluster.traitSet().replace(PhoenixConvention.CLIENT)
                .replaceIfs(RelCollationTraitDef.INSTANCE,
                        new Supplier<List<RelCollation>>() {
                    public List<RelCollation> get() {
                        return RelMdCollation.project(input, projects);
                    }
                });
        return new PhoenixClientProject(cluster, traits, input, projects, rowType);
    }

    private PhoenixClientProject(RelOptCluster cluster, RelTraitSet traits,
            RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
        super(cluster, traits, input, projects, rowType);
    }

    @Override
    public PhoenixClientProject copy(RelTraitSet traits, RelNode input,
            List<RexNode> projects, RelDataType rowType) {
        return create(input, projects, rowType);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        if (!getInput().getConvention().satisfies(PhoenixConvention.SERVERJOIN)
                && !getInput().getConvention().satisfies(PhoenixConvention.CLIENT))
            return planner.getCostFactory().makeInfiniteCost();
        
        return super.computeSelfCost(planner)
                .multiplyBy(PHOENIX_FACTOR);
    }

    @Override
    public QueryPlan implement(Implementor implementor) {
        implementor.pushContext(implementor.getCurrentContext().withColumnRefList(getColumnRefList()));
        QueryPlan plan = implementor.visitInput(0, (PhoenixRel) getInput());
        implementor.popContext();
        
        TupleProjector tupleProjector = project(implementor);
        
        return new TupleProjectionPlan(plan, tupleProjector, null);
    }

}
