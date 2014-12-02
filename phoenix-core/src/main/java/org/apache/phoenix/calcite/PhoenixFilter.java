package org.apache.phoenix.calcite;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.jdbc.PhoenixConnection;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Filter}
 * relational expression in Phoenix.
 */
public class PhoenixFilter extends Filter implements PhoenixRel {
    protected PhoenixFilter(RelOptCluster cluster, RelTraitSet traits, RelNode input, RexNode condition) {
        super(cluster, traits, input, condition);
        assert getConvention() == PhoenixRel.CONVENTION;
        assert getConvention() == input.getConvention();
    }

    public PhoenixFilter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new PhoenixFilter(getCluster(), traitSet, input, condition);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        return super.computeSelfCost(planner).multiplyBy(PHOENIX_FACTOR);
    }

    public void implement(Implementor implementor, PhoenixConnection conn) {
        implementor.visitInput(0, (PhoenixRel) getInput());
        // TODO: what to do with the Expression?
        // Already determined this filter cannot be pushed down, so
        // this will be run 
        Expression expr = CalciteUtils.toExpression(condition, implementor);
    }
}
