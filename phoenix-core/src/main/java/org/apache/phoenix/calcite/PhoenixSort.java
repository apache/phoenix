package org.apache.phoenix.calcite;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;
import org.apache.phoenix.compile.QueryPlan;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Sort}
 * relational expression in Phoenix.
 *
 * <p>Like {@code Sort}, it also supports LIMIT and OFFSET.
 */
public class PhoenixSort extends Sort implements PhoenixRel {
    public PhoenixSort(RelOptCluster cluster, RelTraitSet traits, RelNode child, RelCollation collation, RexNode offset, RexNode fetch) {
        super(cluster, traits, child, collation, offset, fetch);
    }

    @Override
    public PhoenixSort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch) {
        return new PhoenixSort(getCluster(), traitSet, newInput, newCollation, offset, fetch);
    }

    @Override
    public QueryPlan implement(Implementor implementor) {
        implementor.visitInput(0, (PhoenixRel) getInput());
        throw new UnsupportedOperationException();
    }

    @Override
    public PlanType getPlanType() {
        return PlanType.CLIENT_SERVER;
    }
}
