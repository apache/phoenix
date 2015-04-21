package org.apache.phoenix.calcite.rel;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.phoenix.calcite.metadata.PhoenixRelMdCollation;
import org.apache.phoenix.compile.QueryPlan;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

public class PhoenixClientJoin extends PhoenixAbstractJoin {
    
    public static PhoenixClientJoin create(RelNode left, RelNode right, 
            RexNode condition, JoinRelType joinType, Set<String> variablesStopped) {
        RelOptCluster cluster = left.getCluster();
        final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
        final RelNode sortedLeft = sortInput(left, joinInfo.leftKeys);
        final RelNode sortedRight = sortInput(right, joinInfo.rightKeys);
        final RelTraitSet traits =
                cluster.traitSet().replace(PhoenixRel.CONVENTION)
                .replaceIfs(RelCollationTraitDef.INSTANCE,
                        new Supplier<List<RelCollation>>() {
                    public List<RelCollation> get() {
                        return PhoenixRelMdCollation.mergeJoin(sortedLeft, sortedRight, joinInfo.leftKeys, joinInfo.rightKeys);
                    }
                });
        return new PhoenixClientJoin(cluster, traits, sortedLeft, sortedRight, condition, joinType, variablesStopped);
    }
    
    private static RelNode sortInput(RelNode input, ImmutableIntList sortKeys) {
        if (sortKeys.isEmpty()) {
            return input;
        }
        
        List<RelFieldCollation> fieldCollations = Lists.newArrayList();
        for (Iterator<Integer> iter = sortKeys.iterator(); iter.hasNext();) {
            fieldCollations.add(new RelFieldCollation(iter.next(), Direction.ASCENDING));
        }
        RelCollation collation = RelCollations.of(fieldCollations);
        List<RelCollation> collations = input.getTraitSet().getTraits(RelCollationTraitDef.INSTANCE);
        if (collations.contains(collation)) {
            return input;
        }
        
        return PhoenixClientSort.create(input, collation);
    }

    private PhoenixClientJoin(RelOptCluster cluster, RelTraitSet traits,
            RelNode left, RelNode right, RexNode condition,
            JoinRelType joinType, Set<String> variablesStopped) {
        super(cluster, traits, left, right, condition, joinType,
                variablesStopped);
    }

    @Override
    public PhoenixClientJoin copy(RelTraitSet traits, RexNode condition, RelNode left,
            RelNode right, JoinRelType joinRelType, boolean semiJoinDone) {
        return create(left, right, condition, joinRelType, variablesStopped);
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
