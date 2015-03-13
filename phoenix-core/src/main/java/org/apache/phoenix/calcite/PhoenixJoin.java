package org.apache.phoenix.calcite;

import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.phoenix.compile.QueryPlan;

import com.google.common.collect.ImmutableSet;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Join}
 * relational expression in Phoenix.
 */
public class PhoenixJoin extends Join implements PhoenixRel {
    public PhoenixJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition, JoinRelType joinType, Set<String> variablesStopped) {
        super( cluster, traits, left, right, condition, joinType, variablesStopped);
        assert getConvention() == PhoenixRel.CONVENTION;
        assert left.getConvention() == PhoenixRel.CONVENTION;
        assert right.getConvention() == PhoenixRel.CONVENTION;
    }

    @Override
    public PhoenixJoin copy(RelTraitSet traits, RexNode condition, RelNode left, RelNode right, JoinRelType joinRelType, boolean semiJoinDone) {
        return new PhoenixJoin(getCluster(), traits, left, right, condition, joinRelType, ImmutableSet.<String>of());
    }

    @Override
    public QueryPlan implement(Implementor implementor) {
        throw new UnsupportedOperationException();
    }
}
