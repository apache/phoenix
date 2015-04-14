package org.apache.phoenix.calcite.rel;

import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.phoenix.compile.QueryPlan;

import com.google.common.collect.ImmutableSet;

public class PhoenixJoin extends Join implements PhoenixRel {

    public PhoenixJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left,
            RelNode right, RexNode condition, JoinRelType joinType,
            Set<String> variablesStopped) {
        super(cluster, traits, left, right, condition, joinType,
                variablesStopped);
    }

    @Override
    public Join copy(RelTraitSet traits, RexNode condition, RelNode left,
            RelNode right, JoinRelType joinRelType, boolean semiJoinDone) {
        return new PhoenixJoin(getCluster(), traits, left, right, condition, joinRelType, ImmutableSet.<String>of());
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        return planner.getCostFactory().makeCost(Double.POSITIVE_INFINITY, 0, 0);
    }

    @Override
    public QueryPlan implement(Implementor implementor) {
        throw new UnsupportedOperationException();
    }

}
