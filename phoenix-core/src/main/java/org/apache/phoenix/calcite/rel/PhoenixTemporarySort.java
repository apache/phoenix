package org.apache.phoenix.calcite.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.phoenix.compile.QueryPlan;

public class PhoenixTemporarySort extends PhoenixAbstractSort {

    public static PhoenixTemporarySort create(RelNode input, RelCollation collation) {
        RelOptCluster cluster = input.getCluster();
        collation = RelCollationTraitDef.INSTANCE.canonize(collation);
        RelTraitSet traits =
                input.getTraitSet().replace(input.getConvention()).replace(collation);
        return new PhoenixTemporarySort(cluster, traits, input, collation);
    }

    private PhoenixTemporarySort(RelOptCluster cluster, RelTraitSet traits,
            RelNode child, RelCollation collation) {
        super(cluster, traits, child, collation);
    }

    @Override
    public PhoenixTemporarySort copy(RelTraitSet traitSet, RelNode newInput,
            RelCollation newCollation, RexNode offset, RexNode fetch) {
        return create(newInput, newCollation);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeInfiniteCost();
    }

    @Override
    public QueryPlan implement(PhoenixRelImplementor implementor) {
        throw new UnsupportedOperationException();
    }
}