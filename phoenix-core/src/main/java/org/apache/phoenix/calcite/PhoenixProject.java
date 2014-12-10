package org.apache.phoenix.calcite;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.phoenix.jdbc.PhoenixConnection;

import java.util.List;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Project}
 * relational expression in Phoenix.
 */
public class PhoenixProject extends Project implements PhoenixRel {
    public PhoenixProject(RelOptCluster cluster, RelTraitSet traits, RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
        super(cluster, traits, input, projects, rowType, Flags.BOXED);
        assert getConvention() == PhoenixRel.CONVENTION;
        assert getConvention() == input.getConvention();
    }

    @Override
    public PhoenixProject copy(RelTraitSet traits, RelNode input, List<RexNode> projects, RelDataType rowType) {
        return new PhoenixProject(getCluster(), traits, input, projects, rowType);
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        return super.computeSelfCost(planner).multiplyBy(PHOENIX_FACTOR);
    }

    @Override
    public void implement(Implementor implementor, PhoenixConnection conn) {
    	implementor.setProjects(getProjects());
        implementor.visitInput(0, (PhoenixRel) getInput());
    }
}
