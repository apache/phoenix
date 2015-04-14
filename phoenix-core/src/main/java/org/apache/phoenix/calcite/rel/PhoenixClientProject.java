package org.apache.phoenix.calcite.rel;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.execute.TupleProjectionPlan;
import org.apache.phoenix.execute.TupleProjector;

public class PhoenixClientProject extends PhoenixAbstractProject {

    public PhoenixClientProject(RelOptCluster cluster, RelTraitSet traits,
            RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
        super(cluster, traits, input, projects, rowType);
    }

    @Override
    public PhoenixClientProject copy(RelTraitSet traits, RelNode input,
            List<RexNode> projects, RelDataType rowType) {
        return new PhoenixClientProject(getCluster(), traits, input, projects, rowType);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        return super.computeSelfCost(planner)
                .multiplyBy(PHOENIX_FACTOR);
    }

    @Override
    public QueryPlan implement(Implementor implementor) {
        assert getConvention() == getInput().getConvention();
        
        QueryPlan plan = implementor.visitInput(0, (PhoenixRel) getInput());        
        TupleProjector tupleProjector = project(implementor);
        
        return new TupleProjectionPlan(plan, tupleProjector, null);
    }

}
