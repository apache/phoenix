package org.apache.phoenix.calcite.rel;

import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.phoenix.compile.QueryPlan;

import com.google.common.collect.ImmutableSet;

public class PhoenixClientJoin extends PhoenixAbstractJoin {

    public PhoenixClientJoin(RelOptCluster cluster, RelTraitSet traits,
            RelNode left, RelNode right, RexNode condition,
            JoinRelType joinType, Set<String> variablesStopped) {
        super(cluster, traits, left, right, condition, joinType,
                variablesStopped);
    }

    @Override
    public PhoenixClientJoin copy(RelTraitSet traits, RexNode condition, RelNode left,
            RelNode right, JoinRelType joinRelType, boolean semiJoinDone) {
        return new PhoenixClientJoin(getCluster(), traits, left, right, condition, joinRelType, ImmutableSet.<String>of());
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        double rowCount = RelMetadataQuery.getRowCount(this);
        
        for (RelNode input : getInputs()) {
            double inputRowCount = input.getRows();
            if (Double.isInfinite(inputRowCount)) {
                rowCount = inputRowCount;
            } else {
                rowCount += inputRowCount;
            }
        }
        RelOptCost cost = planner.getCostFactory().makeCost(rowCount, 0, 0);

        return cost.multiplyBy(PHOENIX_FACTOR);
    }

    @Override
    public QueryPlan implement(Implementor implementor) {
        throw new UnsupportedOperationException();
    }

}
