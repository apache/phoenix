package org.apache.phoenix.calcite.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;
import org.apache.phoenix.calcite.CalciteUtils;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.execute.ClientScanPlan;
import org.apache.phoenix.expression.Expression;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Filter}
 * relational expression in Phoenix.
 */
public class PhoenixFilter extends Filter implements PhoenixRel {
    public PhoenixFilter(RelOptCluster cluster, RelTraitSet traits, RelNode input, RexNode condition) {
        super(cluster, traits, input, condition);
        assert getConvention() == PhoenixRel.CONVENTION;
    }

    public PhoenixFilter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new PhoenixFilter(getCluster(), traitSet, input, condition);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        return super.computeSelfCost(planner).multiplyBy(PHOENIX_FACTOR);
    }

    public QueryPlan implement(Implementor implementor) {
        assert getConvention() == getInput().getConvention();
        QueryPlan plan = implementor.visitInput(0, (PhoenixRel) getInput());
        Expression expr = CalciteUtils.toExpression(condition, implementor);
        return new ClientScanPlan(plan.getContext(), plan.getStatement(), plan.getTableRef(),
                plan.getProjector(), null, expr, OrderBy.EMPTY_ORDER_BY, plan);
    }
}
